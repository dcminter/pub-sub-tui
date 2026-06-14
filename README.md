# Pub-Sub TUI

This is a TUI (Text User Interface) for Google pub/sub, allowing monitoring of the metadata
of a topics on (typically) a mock pub/sub instance running locally. In theory it could
run against real pub/sub instances also.

## Features

  * A running count of the number of topics on the current instance
  * A tree-view of topics on the current instance that have non-zero message counts
  * Notes in the tree-view also note
    * Which publishers exist on the topic
      * For each, how many messages they have published
      * Publishers to a topic with a zero-message published count will NOT be shown
    * Which consumers exist on the topic
      * For each, how many messages they have consumed
      * Consumers from a topic with a zero-message consumed count will NOT be shown
  * Basic statistics on how many publishers/consumers in total are connected to the pub/sub instance
  * The TUI handles re-sizes of the terminal window automatically
  * The TUI colour scheme apes the old-skool Borland IDE style!
  * The tool only reads metadata - applications running against the same pub/sub instance will be
    completely unaffected.

## Tooling

  * The pub/sub mock to be used is `google/cloud-sdk:emulators` (a Docker image)
  * The ratatui crate (along with crossterm) is used for UI rendering
  * The clap crate is used for command-line rendering
    * Declarative mode is used for the Clap tool - all clap config is therefore in the Rust source
  * The google-cloud-pubsub crate is used for pub/sub metadata access
