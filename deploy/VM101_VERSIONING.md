# VM101 image version contract

VM101 custom images must use one version string for both the Docker image tag and
the version embedded in the Sub2API binary:

```text
X.Y.Z-yms-MMDD-N
```

Build the image only through the checked-in target:

```bash
YMS_BUILD_SEQ=1 make build-vm101-image
```

The base version is read from `backend/cmd/server/VERSION`. The build target
generates the complete custom version once, passes it to Docker as both the image
tag and `VERSION` build argument, and then runs the resulting image to verify its
architecture and embedded version. A mismatch fails the build before transfer or
deployment.

For a same-day rebuild, increment `YMS_BUILD_SEQ`. `YMS_BUILD_DATE` and
`BASE_VERSION` may be set explicitly only when reproducing a historical build.

Run the lightweight contract test with:

```bash
make test-vm101-version-contract
```
