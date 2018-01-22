# ETL sbt project for spark

## Build

Note: works with java 7 and 8 (not with jdk 9)

sbt package

## Classes

### io.elegans.etl.GenerateSentencesFromTransactions

#### running using sparkSubmit plugin of sbt

```bash
sbt "sparkSubmit --class io.elegans.etl.GenerateSentencesFromTransactions -- --help"

create sentences from user iteractions list of sentences with spark
Usage: TokenizeSentences [options]

  --help                  prints this usage text
  --users <value>         the users data file  default: users.csv
  --items <value>         the items data file  default: items.csv
  --transactions <value>  the transactions file  default: transactions.csv
  --output <value>        the destination directory for the output  default: SENTENCES
  --format <value>        the format of the the output  default: format1
```

#### running calling spark-submit

```bash
./scripts/run.sh io.elegans.etl.GenerateSentencesFromTransactions --help
```

e.g.

```bash
./scripts/run.sh io.elegans.etl.GenerateSentencesFromTransactions --users dataset/users.csv --items dataset/items.csv --output dataset_out --transactions dataset/transactions.csv --format format4"

```

#### generation of a fat jar

```bash
export JAVA_OPTS="-Xms256m -Xmx4g"
sbt assembly
```

### run the program using the fat jar

```bash
spark-submit --class io.elegans.etl.GenerateSentencesFromTransactions ./target/scala-2.11/etl-assembly-0.1.jar  --help
```

