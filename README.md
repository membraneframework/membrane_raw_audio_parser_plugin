# Membrane Raw Audio Parser Plugin

[![Hex.pm](https://img.shields.io/hexpm/v/membrane_raw_audio_parser_plugin.svg)](https://hex.pm/packages/membrane_raw_audio_parser_plugin)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/membrane_raw_audio_parser_plugin)
[![CircleCI](https://circleci.com/gh/membraneframework/membrane_raw_audio_parser_plugin.svg?style=svg)](https://circleci.com/gh/membraneframework/membrane_raw_audio_parser_plugin)

Plugin providing element for parsing raw audio. 
It will ensure that buffers contain full samples and can overwrite timestamps additionally.

It is part of [Membrane Multimedia Framework](https://membraneframework.org).

## Installation

The package can be installed by adding `membrane_raw_audio_parser_plugin ` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:membrane_raw_audio_parser_plugin, "~> 0.4.1"}
  ]
end
```

## Usage

In this example, two audio sources from the internet are mixed and then passed to the player in real-time.
To link the audio source to LiveMixer each buffer has to have pts.
Source 1 is delayed by 5 seconds from source 2.

```elixir
defmodule Mixing.Pipeline do
  use Membrane.Pipeline

  @impl true
  def handle_init(_ctx, _options) do
    spec = [
      child({:source, 1}, %Membrane.Hackney.Source{
        location:
          "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/beep-s16le-48kHz-stereo.raw",
        hackney_opts: [follow_redirect: true]
      })
      |> child({:parser, 1}, %Membrane.RawAudioParser{
        stream_format: %Membrane.RawAudio{
          channels: 2,
          sample_format: :s16le,
          sample_rate: 48_000
        },
        overwrite_pts?: true,
        pts_offset: Membrane.Time.seconds(5)
      })
      |> get_child(:mixer),
      child({:source, 2}, %Membrane.Hackney.Source{
        location:
          "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/beep-s16le-48kHz-stereo.raw",
        hackney_opts: [follow_redirect: true]
      })
      |> child({:parser, 2}, %Membrane.RawAudioParser{
        stream_format: %Membrane.RawAudio{
          channels: 2,
          sample_format: :s16le,
          sample_rate: 48_000
        },
        overwrite_pts?: true
      })
      |> get_child(:mixer),
      child(:mixer, Membrane.LiveAudioMixer)
      |> child(:player, Membrane.PortAudio.Sink)
    ]

    {[spec: spec], %{}}
  end
end
```

## Copyright and License

Copyright 2023, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane)

[![Software Mansion](https://logo.swmansion.com/logo?color=white&variant=desktop&width=200&tag=membrane-github)](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane)

Licensed under the [Apache License, Version 2.0](LICENSE)
