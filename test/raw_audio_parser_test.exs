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

  @silence_10ms RawAudio.silence(@stream_format, Time.milliseconds(10))

  describe "for sample-aligned streams" do
    defp run_test_pipeline(
           %RawAudioParser{} = parser_spec,
           input_count,
           input_duration,
           init_pts \\ nil
         ) do
      buffers =
        0..(input_count - 1)//1
        |> Stream.map(fn i ->
          pts =
            case init_pts do
              nil -> nil
              _init_pts -> init_pts + i * input_duration
            end

          %Buffer{
            payload: RawAudio.silence(@stream_format, input_duration),
            pts: pts,
            metadata: %{index: i}
          }
        end)

      spec = [
        child(:source, %Source{
          output: buffers,
          stream_format: @stream_format
        })
        |> child(:parser, parser_spec)
        |> child(:sink, Sink)
      ]

      assert pipeline = Pipeline.start_link_supervised!(spec: spec)
      assert_end_of_stream(pipeline, :sink)
      pipeline
    end

    test "parser adds timestamps" do
      buffer_duration = Time.milliseconds(10)
      pipeline = run_test_pipeline(%RawAudioParser{overwrite_pts?: true}, 10, buffer_duration)

      for i <- 0..9 do
        pts = i * buffer_duration

        assert_sink_buffer(
          pipeline,
          :sink,
          %Buffer{
            pts: ^pts,
            payload: @silence_10ms,
            metadata: %{index: ^i}
          },
          0
        )
      end

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser adds timestamps with offset" do
      buffer_duration = Time.milliseconds(10)
      offset = 10

      pipeline =
        run_test_pipeline(
          %RawAudioParser{overwrite_pts?: true, pts_offset: offset},
          10,
          buffer_duration
        )

      for i <- 0..9 do
        pts = i * buffer_duration + offset

        assert_sink_buffer(
          pipeline,
          :sink,
          %Buffer{
            pts: ^pts,
            payload: @silence_10ms,
            metadata: %{index: ^i}
          },
          0
        )
      end

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser merges payloads into chunks of `chunk_duration`" do
      offset = 10
      chunk_duration = Time.milliseconds(50)

      pipeline =
        run_test_pipeline(
          %RawAudioParser{chunk_duration: chunk_duration, overwrite_pts?: false},
          10,
          Time.milliseconds(10),
          offset
        )

      chunk = RawAudio.silence(@stream_format, chunk_duration)

      for i <- 0..1 do
        offset = 10
        pts = i * chunk_duration + offset

        assert_sink_buffer(
          pipeline,
          :sink,
          %Buffer{
            pts: ^pts,
            payload: ^chunk
          },
          0
        )
      end

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser merges payloads into chunks of `chunk_duration` and assigns correct pts" do
      chunk_duration = Time.milliseconds(50)

      pipeline =
        run_test_pipeline(
          %RawAudioParser{chunk_duration: chunk_duration, overwrite_pts?: true},
          10,
          Time.milliseconds(10)
        )

      chunk = RawAudio.silence(@stream_format, chunk_duration)

      for i <- 0..1 do
        pts = i * chunk_duration

        assert_sink_buffer(
          pipeline,
          :sink,
          %Buffer{
            payload: ^chunk,
            pts: ^pts
          },
          0
        )
      end

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser splits payloads into chunks of `chunk_duration`" do
      offset = 10
      chunk_duration = Time.milliseconds(20)

      pipeline =
        run_test_pipeline(
          %RawAudioParser{chunk_duration: chunk_duration, overwrite_pts?: false},
          2,
          Time.milliseconds(50),
          offset
        )

      chunk = RawAudio.silence(@stream_format, chunk_duration)

      for i <- 0..4 do
        pts = i * chunk_duration + offset
        assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^chunk, pts: ^pts}, 0)
      end

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser splits payloads into chunks of `chunk_duration` and generates correct pts" do
      chunk_duration = Time.milliseconds(20)

      pipeline =
        run_test_pipeline(
          %RawAudioParser{chunk_duration: chunk_duration, overwrite_pts?: true},
          2,
          Time.milliseconds(50)
        )

      chunk = RawAudio.silence(@stream_format, chunk_duration)

      for i <- 0..4 do
        pts = i * chunk_duration

        assert_sink_buffer(
          pipeline,
          :sink,
          %Buffer{
            payload: ^chunk,
            pts: ^pts
          },
          0
        )
      end

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser splits payloads into chunks of `chunk_duration` and recomputes input pts properly" do
      chunk_duration = Time.milliseconds(20)
      offset = 10

      pipeline =
        run_test_pipeline(
          %RawAudioParser{chunk_duration: chunk_duration, overwrite_pts?: false},
          2,
          Time.milliseconds(50),
          offset
        )

      chunk = RawAudio.silence(@stream_format, chunk_duration)

      for i <- 0..4 do
        pts = i * chunk_duration + offset

        assert_sink_buffer(
          pipeline,
          :sink,
          %Buffer{
            payload: ^chunk,
            pts: ^pts
          },
          0
        )
      end

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser flushes the sub-chunk remainder at end of stream" do
      offset = 10
      chunk_duration = Time.milliseconds(30)

      pipeline =
        run_test_pipeline(
          %RawAudioParser{chunk_duration: chunk_duration},
          10,
          Time.milliseconds(10),
          offset
        )

      chunk = RawAudio.silence(@stream_format, chunk_duration)
      remainder = RawAudio.silence(@stream_format, Time.milliseconds(10))

      for i <- 0..2 do
        pts = i * chunk_duration + offset
        assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^chunk, pts: ^pts}, 0)
      end

      assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^remainder}, 0)
      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser splits input buffers into chunks" do
      chunk_duration = Time.milliseconds(20)

      pipeline =
        run_test_pipeline(
          %RawAudioParser{
            chunk_duration: chunk_duration,
            overwrite_pts?: true
          },
          5,
          Time.milliseconds(30)
        )

      chunk = RawAudio.silence(@stream_format, chunk_duration)

      for i <- 0..6 do
        pts = i * chunk_duration

        assert_sink_buffer(
          pipeline,
          :sink,
          %Buffer{
            payload: ^chunk,
            pts: ^pts
          },
          0
        )
      end

      pts = 7 * chunk_duration
      chunk = RawAudio.silence(@stream_format, Time.milliseconds(10))

      assert_sink_buffer(
        pipeline,
        :sink,
        %Buffer{
          payload: ^chunk,
          pts: ^pts
        },
        0
      )

      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser drops a trailing remainder smaller than one sample at end of stream" do
      chunk_duration = Time.milliseconds(10)
      silence = @silence_10ms
      payload = silence <> <<0, 0, 0>>

      spec = [
        child(:source, %Source{output: [payload], stream_format: @stream_format})
        |> child(:parser, %RawAudioParser{chunk_duration: chunk_duration})
        |> child(:sink, Sink)
      ]

      assert pipeline = Pipeline.start_link_supervised!(spec: spec)
      assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^silence})
      assert_end_of_stream(pipeline, :sink)
      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser re-aligns a multi-sample remainder to whole samples at end of stream" do
      chunk_duration = Time.milliseconds(10)
      chunk = RawAudio.silence(@stream_format, chunk_duration)

      whole_frame = <<1, 2, 3, 4, 5, 6>>
      sub_sample_tail = <<7>>

      payload = chunk <> whole_frame <> sub_sample_tail

      spec = [
        child(:source, %Source{output: [payload], stream_format: @stream_format})
        |> child(:parser, %RawAudioParser{chunk_duration: chunk_duration})
        |> child(:sink, Sink)
      ]

      assert pipeline = Pipeline.start_link_supervised!(spec: spec)
      assert_end_of_stream(pipeline, :sink)

      assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^chunk}, 0)
      assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^whole_frame}, 0)
      refute_sink_buffer(pipeline, :sink, _buffer, 0)
    end

    test "parser divides payloads into samples" do
      payload_bytes = div(byte_size(@silence_10ms), 2)
      <<payload::binary-size(^payload_bytes), _rest::binary>> = @silence_10ms

      buffers = for _i <- 1..9, do: payload

      spec = [
        child(:source, %Source{output: buffers, stream_format: @stream_format})
        |> child(:parser, RawAudioParser)
        |> child(:sink, Sink)
      ]

      assert pipeline = Pipeline.start_link_supervised!(spec: spec)
      assert_end_of_stream(pipeline, :sink)

      silence_duration = @silence_10ms |> byte_size() |> RawAudio.bytes_to_time(@stream_format)
      extended_payload = RawAudio.silence(@stream_format, div(silence_duration, 2))

      <<^extended_payload::binary-size(byte_size(^extended_payload)), truncated_payload::binary>> =
        @silence_10ms

      # Each buffer that Parser gets has some amount of whole samples and half of the sample at the end.
      # Half of the buffers should be truncated
      # and half of them will be extended by truncated part from the previous buffer.
      # This is because Parser ensures that all buffers have only whole samples.
      for _i <- 1..5,
          do:
            assert_sink_buffer(pipeline, :sink, %Buffer{pts: nil, payload: ^truncated_payload}, 0)

      for _i <- 1..4,
          do:
            assert_sink_buffer(pipeline, :sink, %Buffer{pts: nil, payload: ^extended_payload}, 0)
    end

    test "parser can have `RemoteStream` as input" do
      spec = [
        child(:source, %Membrane.File.Source{location: "test/fixtures/beep.raw"})
        |> child(:parser, %RawAudioParser{assumed_input_stream_format: @stream_format})
        |> child(:sink, Sink)
      ]

      assert pipeline = Pipeline.start_link_supervised!(spec: spec)
      assert_end_of_stream(pipeline, :sink)
    end
  end
end
