Prototype of an implementation of the Spark Shuffle plugin that is being developed as part of SPARK-25299.

This project was developed as a proof of concept in a Palantir hack week and is available for theoretical discussion in the Spark community. Most importantly, this work has not been tested in production nor at scale. Rather, this work shows the potential alternative implementations of remote shuffle storage that the proposed APIs make possible.

This project requires locally building and publishing Spark with the shuffle plugin as proposed in [this PR](https://github.com/mccheah/spark/pull/4).

This work is made available under the [Apache 2.0 License](https://github.com/mccheah/ignite-shuffle-service/blob/master/LICENSE).
