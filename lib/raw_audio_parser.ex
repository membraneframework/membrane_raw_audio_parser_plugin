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

    {[], state}
  end

  @impl true
  def handle_stream_format(
        _pad,
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
        _pad,
        %Membrane.Buffer{payload: payload} = buffer,
        _context,
        %{stream_format: stream_format, chunk_size: chunk_size, acc: acc} = state
      ) do
    payload = acc <> payload
    sample_size = RawAudio.sample_size(stream_format) * stream_format.channels

    acc_size = rem(byte_size(payload), max(sample_size, chunk_size || 0))
    aligned_payload_bytes = byte_size(payload) - acc_size

    <<aligned_payload::binary-size(^aligned_payload_bytes), acc::binary-size(^acc_size)>> =
      payload

    state = %{state | acc: acc}

    if aligned_payload == <<>> do
      {[], state}
    else
      chunk_buffers(buffer, aligned_payload, state)
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

  defp chunk_buffers(
         buffer,
         aligned_payload,
         %{
           overwrite_pts?: overwrite_pts?,
           chunk_size: chunk_size,
           next_pts: next_pts,
           stream_format: stream_format
         } =
           state
       ) do
    chunked_buffers =
      aligned_payload
      |> chunk_payload(chunk_size)
      |> Enum.map(&%{buffer | payload: &1})

    {buffers, state} =
      if overwrite_pts? do
        duration =
          chunked_buffers
          |> hd()
          |> then(& &1.payload)
          |> byte_size()
          |> RawAudio.bytes_to_time(stream_format)

        timestamps = Stream.iterate(next_pts, fn pts -> pts + duration end)

        buffers =
          Enum.zip_with(chunked_buffers, timestamps, fn %Membrane.Buffer{} = buffer, pts ->
            %{buffer | pts: pts}
          end)

        {buffers, %{state | next_pts: next_pts + duration * length(buffers)}}
      else
        {chunked_buffers, state}
      end

    {[buffer: {:output, buffers}], state}
  end

  @spec chunk_payload(binary(), nil | pos_integer()) :: [binary()]
  defp chunk_payload(payload, nil), do: [payload]

  defp chunk_payload(payload, chunk_size) do
    for <<chunk::size(^chunk_size) <- payload>>, do: <<chunk::size(chunk_size)>>
  end
end
