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

  test "parser merges payloads into chunks of `chunk_duration`" do
    chunk_duration = Time.milliseconds(50)
    # 10 buffers of 10 ms each -> 100 ms of audio total
    silence = @silence
    silence_duration = @silence_duration
    buffers =
      Stream.unfold(Time.milliseconds(0), fn pts ->
        {%Membrane.Buffer{payload: silence, pts: pts}, pts + silence_duration}
      end)
      |> Stream.take(10)

    spec = [
      child(:source, %Source{
        output: buffers,
        stream_format: @stream_format
      })
      |> child(:parser, %RawAudioParser{chunk_duration: chunk_duration})
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    chunk = RawAudio.silence(@stream_format, chunk_duration)
    IO.inspect(byte_size(chunk))

    # 100 ms repackaged into 50 ms chunks -> exactly 2 buffers, no remainder
    for i <- 0..1 do
      pts = i * chunk_duration
      assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^chunk, pts: ^pts})
    end

    refute_sink_buffer(pipeline, :sink, _buffer, 0)
  end

  test "parser splits payloads into chunks of `chunk_duration`" do
    chunk_duration = Time.milliseconds(10)
    big_payload = RawAudio.silence(@stream_format, Time.milliseconds(50))
    # 2 buffers of 50 ms each -> 100 ms of audio total
    buffers = [big_payload, big_payload]

    spec = [
      child(:source, %Source{output: buffers, stream_format: @stream_format})
      |> child(:parser, %RawAudioParser{chunk_duration: chunk_duration})
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    chunk = RawAudio.silence(@stream_format, chunk_duration)

    # 100 ms split into 10 ms chunks -> exactly 10 buffers, no remainder
    for i <- 0..9 do
      pts = i * Time.milliseconds(10)
      assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^chunk, pts: ^pts})
    end

    refute_sink_buffer(pipeline, :sink, _buffer, 0)
  end

  test "parser flushes the sub-chunk remainder at end of stream" do
    chunk_duration = Time.milliseconds(30)
    # 10 buffers of 10 ms each -> 100 ms of audio total
    buffers = Enum.map(1..10, fn _idx -> @silence end)

    spec = [
      child(:source, %Source{output: buffers, stream_format: @stream_format})
      |> child(:parser, %RawAudioParser{chunk_duration: chunk_duration})
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    chunk = RawAudio.silence(@stream_format, chunk_duration)
    remainder = RawAudio.silence(@stream_format, Time.milliseconds(10))

    # 100 ms -> 3 full 30 ms chunks (90 ms) + a final 10 ms remainder flushed on EOS
    for _i <- 1..3,
        do: assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^chunk})

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^remainder})
  end

  test "parser adds timestamps to chunks of `chunk_duration`" do
    chunk_duration = Time.milliseconds(20)
    # 10 buffers of 10 ms each -> 100 ms of audio total
    buffers = Enum.map(1..10, fn _idx -> @silence end)

    spec = [
      child(:source, %Source{output: buffers, stream_format: @stream_format})
      |> child(:parser, %RawAudioParser{chunk_duration: chunk_duration, overwrite_pts?: true})
      |> child(:sink, Sink)
    ]

    assert pipeline = Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    chunk = RawAudio.silence(@stream_format, chunk_duration)

    # 100 ms split into 20 ms chunks -> 5 buffers, each pts advanced by one chunk_duration
    for i <- 0..4 do
      pts = i * chunk_duration
      assert_sink_buffer(pipeline, :sink, %Buffer{pts: ^pts, payload: ^chunk})
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
