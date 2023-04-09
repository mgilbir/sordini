# sordini

This project started as fork of [Jocko](https://github.com/travisjeffery/jocko).

The changes remove functionality that was present in Jocko, and extends it in very specific ways to make it fit the needs of having an embeddable kafka protocol message sink.

It allows receiving messages from a Kafka producer, and exposes them to the embedding program via a callback.

All message persistence, or attempts to make it work as a distributed log have been removed.

## What's in a name

Sordini is a character in Franz Kafka's novel "The Castle," who serves as an intermediary between various parties. Like Sordini, this project facilitates the exchange of messages and information, acting as an intermediary by receiving messages from a Kafka producer and making them available through a callback function. Though Sordini is part of a complex bureaucratic system, his role in the novel highlights the importance of communication and the flow of information, which is central to the functionality of the project.

Additionally in italian, "sordini" is the plural form of "sordino," which translates to "mute" or "damper" in English. These terms are often used in the context of musical instruments to muffle or dampen the sound. This is a nod to the fact that the project receives messages from the Kafka protocol but does not send information back to kafka consumers, thereby acting as a "mute" or "one-way" communication system.

## License

sordini is released under the MIT license, see the [LICENSE](LICENSE) file for details.
