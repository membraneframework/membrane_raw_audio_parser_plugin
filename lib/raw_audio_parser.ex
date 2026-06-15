defmodule Membrane.RawAudioParser do
  @moduledoc """
  This element is responsible for parsing audio in RawAudio format.
  The Parser ensures that output buffers have whole samples.

  By default the parser doesn't ensure that each output buffer holds the same
  number of samples - it only re-aligns buffers to whole samples. When
  `chunk_duration` is set, the parser additionally re-chunks the stream so that
  every output buffer carries exactly `chunk_duration` worth of audio (the very
  last buffer, flushed at end of stream, may be shorter).
  """

  use Membrane.Filter

  alias Membrane.{Buffer, RawAudio, RemoteStream}

  def_options stream_format: [
                spec: RawAudio.t() | nil,
                description: """
                Defines a raw audio format of the input pad.
                """,
                default: nil
              ],
              overwrite_pts?: [
                spec: boolean(),
                description: """
                If set to true, RawAudioParser will add timestamps based on payload duration.
                """,
                default: false
              ],
              pts_offset: [
                spec: non_neg_integer(),
                description: """
                If set to a value different than 0,
                RawAudioParser will start timestamps from this offset.
                Only valid when `overwrite_pts?` is set to true.
                """,
                default: 0
              ],
              chunk_duration: [
                spec: Membrane.Time.t() | nil,
                description: """
                When set, output buffers are re-chunked so that each one carries exactly
                `chunk_duration` worth of audio. Bytes that don't fill a whole chunk are
                buffered until enough data arrives; the trailing remainder is flushed as a
                (possibly shorter) buffer at end of stream.

                When `nil` (the default) the parser only re-aligns buffers to whole samples
                and otherwise passes them through unchanged.
                """,
                default: nil
              ],
              metadata_placement_strategy: [
                spec: :first_buffer_only | :repeat_in_chunks,
                description: """
                Controls how the metadata of an input buffer is propagated to the chunks
                produced from it. Only relevant when `chunk_duration` is set.

                - `:first_buffer_only` - within a single emitted batch only the
                  first chunk carries metadata; the remaining chunks have empty metadata.
                - `:repeat_in_chunks` - every produced chunk carries the metadata of the
                  input buffer it originates from.
                """,
                default: :first_buffer_only
              ]

  def_input_pad :input,
    flow_control: :auto,
    accepted_format:
      any_of(
        Membrane.RawAudio,
        Membrane.RemoteStream
      ),
    availability: :always

  def_output_pad :output,
    flow_control: :auto,
    availability: :always,
    accepted_format: Membrane.RawAudio

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{
            stream_format: RawAudio.t() | nil,
            overwrite_pts?: boolean(),
            pts_offset: non_neg_integer(),
            chunk_duration: Membrane.Time.t() | nil,
            metadata_placement_strategy: :first_buffer_only | :repeat_in_chunks,
            # The pts the next emitted output buffer should carry
            # (generated in overwrite mode, or remembered from the input in passthrough mode).
            # May be `nil` until the first timestamp is known.
            next_pts: Membrane.Time.t() | nil,
            # Bytes that didn't fill a whole alignment unit (chunk or frame) yet,
            # carried to the next buffer.
            acc: binary(),
            frame_size: pos_integer() | nil,
            chunk_size: pos_integer() | nil,
            # Metadata of the most recently emitted input buffer, used to tag chunks that
            # start with leftover bytes from the previous input buffer.
            last_metadata: Buffer.metadata()
          }

    defstruct [
      :stream_format,
      :overwrite_pts?,
      :pts_offset,
      :chunk_duration,
      :metadata_placement_strategy,
      :next_pts,
      :frame_size,
      :chunk_size,
      acc: <<>>,
      last_metadata: %{}
    ]
  end

  @impl true
  def handle_init(_ctx, options) do
    state = %State{
      stream_format: options.stream_format,
      overwrite_pts?: options.overwrite_pts?,
      pts_offset: options.pts_offset,
      chunk_duration: options.chunk_duration,
      metadata_placement_strategy: options.metadata_placement_strategy,
      next_pts: options.pts_offset
    }

    {[], state}
  end

  @impl true
  def handle_stream_format(:input, input_stream_format, _ctx, state) do
    {stream_format, state} = resolve_stream_format(input_stream_format, state)

    frame_size = RawAudio.frame_size(stream_format)

    chunk_size =
      if state.chunk_duration,
        do: RawAudio.time_to_bytes(state.chunk_duration, stream_format)

    state = %{state | frame_size: frame_size, chunk_size: chunk_size}
    {[stream_format: {:output, stream_format}], state}
  end

  @impl true
  def handle_buffer(:input, %Buffer{payload: payload} = buffer, _ctx, state) do
    fresh_run? = state.acc == <<>>
    {aligned, leftover} = take_aligned(state.acc <> payload, state)
    state = %{state | acc: leftover}

    cond do
      aligned == <<>> ->
        {[], remember_run_start(state, buffer, fresh_run?)}

      is_nil(state.chunk_size) ->
        # no chunking
        {out, state} = passthrough(buffer, aligned, state)
        {[buffer: {:output, out}], state}

      true ->
        {chunks, state} = build_chunks(aligned, buffer, fresh_run?, state)
        {[buffer: {:output, chunks}], state}
    end
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, %State{chunk_size: nil} = state),
    do: {[end_of_stream: :output], state}

  @impl true
  def handle_end_of_stream(:input, _ctx, state) when byte_size(state.acc) < state.frame_size,
    do: {[end_of_stream: :output], state}

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    remainder = %Buffer{payload: state.acc, pts: state.next_pts, metadata: state.last_metadata}
    {[buffer: {:output, remainder}, end_of_stream: :output], %{state | acc: <<>>}}
  end

  @spec take_aligned(binary(), State.t()) :: {binary(), binary()}
  defp take_aligned(payload, state) do
    unit = state.chunk_size || state.frame_size
    leftover_size = rem(byte_size(payload), unit)
    aligned_size = byte_size(payload) - leftover_size

    <<aligned::binary-size(^aligned_size), leftover::binary-size(^leftover_size)>> = payload
    {aligned, leftover}
  end

  # When passing pts through (not overwriting), the timestamp of the buffer that starts
  # a run must be remembered so it ends up in the output buffer
  @spec remember_run_start(State.t(), Buffer.t(), boolean()) :: State.t()
  defp remember_run_start(%State{overwrite_pts?: false} = state, %Buffer{pts: pts}, true),
    do: %{state | next_pts: pts}

  defp remember_run_start(state, _buffer, _fresh_run?), do: state

  @spec passthrough(Buffer.t(), binary(), State.t()) :: {Buffer.t(), State.t()}
  defp passthrough(buffer, aligned, %State{overwrite_pts?: false} = state),
    do: {%{buffer | payload: aligned}, state}

  defp passthrough(buffer, aligned, %State{overwrite_pts?: true} = state) do
    duration = aligned |> byte_size() |> RawAudio.bytes_to_time(state.stream_format)

    {%{buffer | payload: aligned, pts: state.next_pts},
     %{state | next_pts: state.next_pts + duration}}
  end

  @spec build_chunks(binary(), Buffer.t(), boolean(), State.t()) :: {[Buffer.t()], State.t()}
  defp build_chunks(aligned, buffer, fresh_run?, state) do
    chunks =
      aligned
      |> split_into_chunks(state.chunk_size)
      |> tag_metadata(buffer.metadata, fresh_run?, state)

    {chunks, state} = stamp_pts(chunks, buffer.pts, fresh_run?, state)
    {chunks, %{state | last_metadata: buffer.metadata}}
  end

  @spec split_into_chunks(binary(), pos_integer()) :: [Buffer.t()]
  defp split_into_chunks(payload, chunk_size) do
    for <<chunk::binary-size(^chunk_size) <- payload>>, do: %Buffer{payload: chunk}
  end

  # The first chunk of a batch inherits the metadata of the input buffer that owns its
  # leading bytes: the current buffer when the batch starts a fresh run, otherwise the
  # previous input buffer (whose leftover bytes spilled over into this batch).
  @spec tag_metadata([Buffer.t()], Buffer.metadata(), boolean(), State.t()) :: [Buffer.t()]
  defp tag_metadata([first | rest], input_metadata, fresh_run?, state) do
    first = %{first | metadata: if(fresh_run?, do: input_metadata, else: state.last_metadata)}

    rest =
      case state.metadata_placement_strategy do
        :first_buffer_only -> rest
        :repeat_in_chunks -> Enum.map(rest, &%{&1 | metadata: input_metadata})
      end

    [first | rest]
  end

  @spec stamp_pts([Buffer.t()], Membrane.Time.t() | nil, boolean(), State.t()) ::
          {[Buffer.t()], State.t()}
  defp stamp_pts(chunks, input_pts, fresh_run?, state) do
    start_pts =
      if state.overwrite_pts? or not fresh_run? do
        state.next_pts
      else
        input_pts
      end

    case start_pts do
      nil ->
        {chunks, state}

      _pts ->
        chunks =
          chunks
          |> Enum.with_index()
          |> Enum.map(fn {chunk, index} ->
            %{chunk | pts: start_pts + index * state.chunk_duration}
          end)

        next_pts = start_pts + length(chunks) * state.chunk_duration
        {chunks, %{state | next_pts: next_pts}}
    end
  end

  @spec resolve_stream_format(RawAudio.t() | RemoteStream.t(), State.t()) ::
          {RawAudio.t(), State.t()}
  defp resolve_stream_format(input_stream_format, state) do
    case {input_stream_format, state.stream_format} do
      {%RemoteStream{}, nil} ->
        raise """
        You need to specify `stream_format` in options if `Membrane.RemoteStream` will be received on the `:input` pad
        """

      {_input_format, nil} ->
        {input_stream_format, %{state | stream_format: input_stream_format}}

      {%RemoteStream{}, stream_format} ->
        {stream_format, state}

      {stream_format, stream_format} ->
        {stream_format, state}

      _else ->
        raise """
        Stream format on input pad: #{inspect(input_stream_format)} is different than the one passed in option: #{inspect(state.stream_format)}
        """
    end
  end
end
