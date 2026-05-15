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

    {[], state}
  end

  @impl true
  def handle_stream_format(_pad, input_stream_format, _context, state) do
    case {input_stream_format, state.stream_format} do
      {%RemoteStream{}, nil} ->
        raise """
        You need to specify `stream_format` in options if `Membrane.RemoteStream` will be received on the `:input` pad
        """

      {_input_format, nil} ->
        {[stream_format: {:output, input_stream_format}],
         %{state | stream_format: input_stream_format}}

      {%RemoteStream{}, stream_format} ->
        {[stream_format: {:output, stream_format}], state}

      {stream_format, stream_format} ->
        {[stream_format: {:output, stream_format}], state}

      _else ->
        raise """
        Stream format on input pad: #{inspect(input_stream_format)} is different than the one passed in option: #{inspect(state.stream_format)}
        """
    end
  end

  @impl true
  def handle_buffer(_pad, %Membrane.Buffer{} = buffer, _context, state) do
    %{stream_format: stream_format, overwrite_pts?: overwrite_pts?} = state

    payload = state.acc <> buffer.payload
    sample_size = RawAudio.sample_size(stream_format) * stream_format.channels

    parsed_payload_bytes = byte_size(payload) - rem(byte_size(payload), sample_size)

    <<parsed_payload::binary-size(parsed_payload_bytes), acc::binary>> = payload
    state = %{state | acc: acc}

    if parsed_payload == <<>> do
      {[], state}
    else
      parsed_buffer = %Membrane.Buffer{buffer | payload: parsed_payload}

      {parsed_buffer, state} =
        if overwrite_pts?, do: overwrite_pts(parsed_buffer, state), else: {parsed_buffer, state}

      {[buffer: {:output, parsed_buffer}], state}
    end
  end

  defp overwrite_pts(
         %{payload: payload} = buffer,
         %{next_pts: next_pts, stream_format: stream_format} = state
       ) do
    duration = RawAudio.bytes_to_time(byte_size(payload), stream_format)
    {%{buffer | pts: next_pts}, %{state | next_pts: next_pts + duration}}
  end
end
