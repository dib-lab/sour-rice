# sourrice

## Dependencies

- pandas
- doit
- sourmash
- SRA Toolkit (disable file caching)

## Metadata

```
curl ftp://climb.genomics.cn/pub/10.5524/200001_201000/200001/seq_file_mapping_to_SRA.txt | sed 's/\r$//' | perl -ne 's/_(\d)fastq.gz/_$1.fastq.gz/; print' | gzip -c > metadata.tsv.gz
```

## Pipeline execution

```
doit
```
