defmodule RawAudioParserTest do
  @moduledoc false

  use ExUnit.Case, async: true
  use Membrane.Pipeline

  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, RawAudio, RawAudioParser, Time}
  alias Membrane.Testing.{Pipeline, Sink, Source}

  @stream_format %RawAudio{
    channels: 2,
    sample_rate: 44_100,
    sample_format: :s24le
  }

  @silence_duration Time.milliseconds(10)
  @silence RawAudio.silence(@stream_format, @silence_duration)

  test "parser divides payloads into samples" do
    payload_bytes = div(byte_size(@silence), 2)
    <<payload::binary-size(^payload_bytes), _rest::binary>> = @silence

    buffers = for _i <- 1..9, do: payload

    spec = [
      child(:source, %Source{output: buffers, stream_format: @stream_format})
      |> child(:parser, RawAudioParser)
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    extended_payload = RawAudio.silence(@stream_format, div(@silence_duration, 2))

    <<^extended_payload::binary-size(byte_size(^extended_payload)), truncated_payload::binary>> =
      @silence

    # Each buffer that Parser gets has some amount of whole samples and half of the sample at the end.
    # Half of the buffers should be truncated and half of them will be extended by truncated part from the previous buffer.
    # This is because Parser ensures that all buffers have only whole samples.
    for _i <- 1..5,
        do: assert_sink_buffer(pipeline, :sink, %Buffer{pts: nil, payload: ^truncated_payload})

    for _i <- 1..4,
        do: assert_sink_buffer(pipeline, :sink, %Buffer{pts: nil, payload: ^extended_payload})
  end

  test "parser adds timestamps" do
    buffers = Enum.map(1..10, fn _idx -> @silence end)

    spec = [
      child(:source, %Source{output: buffers, stream_format: @stream_format})
      |> child(:parser, %RawAudioParser{overwrite_pts?: true})
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    for i <- 0..9 do
      pts = i * @silence_duration
      assert_sink_buffer(pipeline, :sink, %Buffer{pts: ^pts, payload: @silence})
    end
  end

  test "parser adds timestamps with offset" do
    offset = 10
    buffers = Enum.map(1..10, fn _idx -> @silence end)

    spec = [
      child(:source, %Source{output: buffers, stream_format: @stream_format})
      |> child(:parser, %RawAudioParser{overwrite_pts?: true, pts_offset: offset})
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    for i <- 0..9 do
      pts = i * @silence_duration + offset
      assert_sink_buffer(pipeline, :sink, %Buffer{pts: ^pts, payload: @silence})
    end
  end

  test "parser can have `RemoteStream` as input" do
    spec = [
      child(:source, %Membrane.File.Source{location: "test/fixtures/beep.raw"})
      |> child(:parser, %RawAudioParser{stream_format: @stream_format})
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)
  end
end
