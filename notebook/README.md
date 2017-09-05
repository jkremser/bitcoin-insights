[![Docker build](https://img.shields.io/docker/automated/jkremser/bitcoin-notebook.svg)](https://hub.docker.com/r/jkremser/bitcoin-notebook)
[![Layers info](https://images.microbadger.com/badges/image/jkremser/bitcoin-notebook.svg)](https://microbadger.com/images/jkremser/bitcoin-notebook)

### Building

```bash
make build
```
This will build a docker image with the notebook and the example data.

### Running

```bash
make run
```

Then the notebook is listening on http://localhost:8888 and the password is `developer`.
