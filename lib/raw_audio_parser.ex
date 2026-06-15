defmodule Membrane.RawAudioParser do
  @moduledoc """
  This element is responsible for parsing audio in RawAudio format.
  The Parser ensures that output buffers have whole samples.
  The parser doesn't ensure that in each output buffer, there will be the same number of samples.
  """

  use Membrane.Filter

  alias Membrane.RawAudio
  alias Membrane.RemoteStream

  def_options stream_format: [
                spec: RawAudio.t() | nil,
                description: """
                The value defines a raw audio format of the input pad.
                """,
                default: nil
              ],
              overwrite_pts?: [
                spec: boolean(),
                description: """
                If set to true RawAudioParser will add timestamps based on payload duration
                """,
                default: false
              ],
              pts_offset: [
                spec: non_neg_integer(),
                description: """
                If set to value different than 0, RawAudioParser will start timestamps from offset.
                It's only valid when `overwrite_pts?` is set to true.
                """,
                default: 0
              ],
              chunk_duration: [
                spec: Membrane.Time.t() | nil,
                description: """
                  TODO __jm__
                """,
                default: nil
              ],
              metadata_placement_strategy: [
                spec: :first_buffer_only | :repeat_in_chunks,
                description: """
                  TODO __jm__
                """,
                default: :first_buffer_only
              ]

  def_input_pad :input,
    flow_control: :auto,
    accepted_format:
      any_of(
        RawAudio,
        Membrane.RemoteStream
      ),
    availability: :always

  def_output_pad :output,
    flow_control: :auto,
    availability: :always,
    accepted_format: RawAudio

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.put(:next_pts, options.pts_offset)
      |> Map.put(:acc, <<>>)
      |> Map.put(:chunk_size, nil)
      |> Map.put(:last_metadata, Map.new())

    {[], state}
  end

  @impl true
  def handle_stream_format(
        :input,
        input_stream_format,
        _context,
        %{chunk_duration: chunk_duration} = state
      ) do
    {resolved_sf, state} = resolve_stream_format(input_stream_format, state)

    chunk_size =
      if is_nil(chunk_duration),
        do: nil,
        else: RawAudio.time_to_bytes(chunk_duration, resolved_sf)

    {[stream_format: {:output, resolved_sf}], %{state | chunk_size: chunk_size}}
  end

  @impl true
  def handle_buffer(
        :input,
        %Membrane.Buffer{payload: payload, pts: input_pts, metadata: metadata} = buffer,
        _context,
        %{
          stream_format: stream_format,
          chunk_size: chunk_size,
          acc: acc,
          next_pts: next_pts,
          overwrite_pts?: overwrite_pts?
        } =
          state
      ) do
    acc_empty? = acc == <<>>
    payload = acc <> payload
    sample_size = RawAudio.sample_size(stream_format) * stream_format.channels

    acc_size =
      rem(byte_size(payload), max(sample_size, chunk_size || 0))

    aligned_payload_bytes = byte_size(payload) - acc_size

    <<aligned_payload::binary-size(^aligned_payload_bytes), acc::binary-size(^acc_size)>> =
      payload

    state = %{state | acc: acc}

    cond do
      aligned_payload == <<>> ->
        next_pts =
          if acc_empty? and not overwrite_pts?, do: input_pts, else: next_pts

        {[], %{state | next_pts: next_pts}}

      is_nil(chunk_size) ->
        aligned_buffer = %{buffer | payload: aligned_payload}

        {aligned_buffer, state} =
          if overwrite_pts? do
            duration = aligned_payload |> byte_size() |> RawAudio.bytes_to_time(stream_format)
            {%{aligned_buffer | pts: next_pts}, %{state | next_pts: next_pts + duration}}
          else
            {aligned_buffer, state}
          end

        {[buffer: {:output, aligned_buffer}], state}

      true ->
        chunk_buffers(aligned_payload, input_pts, metadata, acc_empty?, state)
    end
  end

  @impl true
  def handle_end_of_stream(:input, _context, %{chunk_size: nil} = state),
    do: {[end_of_stream: :output], state}

  @impl true
  def handle_end_of_stream(
        :input,
        _context,
        %{
          acc: acc,
          chunk_size: chunk_size,
          next_pts: next_pts,
          # overwrite_pts?: overwrite_pts?,
          last_metadata: last_metadata,
          stream_format: %RawAudio{channels: channels} = stream_format
        } = state
      )
      when not is_nil(chunk_size) do
    sample_size = RawAudio.sample_size(stream_format) * channels
    acc_duration = acc |> byte_size() |> RawAudio.bytes_to_time(stream_format)

    if sample_size <= byte_size(acc) do
      buffer = %Membrane.Buffer{payload: acc, pts: next_pts, metadata: last_metadata}

      next_pts =
        case next_pts do
          nil -> nil
          _next_pts -> next_pts + acc_duration
        end

      {[
         buffer: {:output, buffer},
         end_of_stream: :output
       ], %{state | next_pts: next_pts}}
    else
      {[end_of_stream: :output], state}
    end
  end

  @spec resolve_stream_format(struct(), map()) :: {struct(), map()}
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

  @spec chunk_buffers(binary(), Membrane.Time.t() | nil, map(), boolean(), map()) ::
          {[Membrane.Element.Action.t()], map()}
  defp chunk_buffers(
         aligned_payload,
         input_pts,
         input_metadata,
         acc_empty?,
         %{
           chunk_size: chunk_size,
           chunk_duration: chunk_duration,
           overwrite_pts?: overwrite_pts?,
           metadata_placement_strategy: mps,
           next_pts: next_pts,
           last_metadata: last_metadata
         } =
           state
       ) do
    buffers =
      aligned_payload
      |> chunk_payload(chunk_size)
      |> Enum.map(&%Membrane.Buffer{payload: &1})
      |> write_metadata(last_metadata, input_metadata, acc_empty?, mps)

    init_pts =
      if overwrite_pts? or not acc_empty? do
        next_pts
      else
        input_pts
      end

    {buffers, state} =
      case init_pts do
        nil ->
          {buffers, state}

        _init_pts ->
          buffers = write_pts(buffers, init_pts, chunk_duration)
          state = %{state | next_pts: init_pts + chunk_duration * length(buffers)}
          {buffers, state}
      end

    {[buffer: {:output, buffers}], %{state | last_metadata: input_metadata}}
  end

  @spec write_pts([Membrane.Buffer.t()], Membrane.Time.t(), Membrane.Time.t()) :: [
          Membrane.Buffer.t()
        ]
  defp write_pts(buffers, init_pts, chunk_duration) do
    timestamps = Stream.iterate(init_pts, fn pts -> pts + chunk_duration end)

    Enum.zip_with(buffers, timestamps, fn %Membrane.Buffer{} = buffer, pts ->
      %{buffer | pts: pts}
    end)
  end

  @spec write_metadata(
          buffers :: [
            Membrane.Buffer.t()
          ],
          last_metadata :: map(),
          input_metadata :: map(),
          acc_empty? :: boolean(),
          placement_strategy :: :first_buffer_only | :repeat_in_chunks
        ) :: [Membrane.Buffer.t()]
  defp write_metadata([first | rest], last_metadata, _input_metadata, false, :first_buffer_only),
    do: [%{first | metadata: last_metadata} | rest]

  defp write_metadata([first | rest], _last_metadata, input_metadata, true, :first_buffer_only),
    do: [%{first | metadata: input_metadata} | rest]

  defp write_metadata([first | rest], last_metadata, input_metadata, false, :repeat_in_chunks) do
    first = %{first | metadata: last_metadata}
    rest = Enum.map(rest, fn buffer -> %{buffer | metadata: input_metadata} end)
    [first | rest]
  end

  defp write_metadata(buffers, _last_metadata, input_metadata, true, :repeat_in_chunks),
    do: Enum.map(buffers, fn buffer -> %{buffer | metadata: input_metadata} end)

  @spec chunk_payload(binary(), nil | pos_integer()) :: [binary()]
  defp chunk_payload(payload, nil), do: [payload]

  defp chunk_payload(payload, chunk_size) do
    for <<chunk::size(^chunk_size * 8) <- payload>>, do: <<chunk::size(chunk_size * 8)>>
  end
end
