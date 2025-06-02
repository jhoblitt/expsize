# expsize

## example usage

```bash
go run expsize.go --file rubinobs-raw-lsstcam.txt.xz --after 2025-05-01 --before 2025-05-20
```

## data collection

```bash
./s5cmd --endpoint-url https://s3.cp.lsst.org ls s3://rubinobs-raw-lsstcam/LSSTCam/* | xz -T0 -9 > rubinobs-raw-lsstcam-$(date +"%Y-%m-%d").txt.xz
```
