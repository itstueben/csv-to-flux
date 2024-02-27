#!/usr/bin/env ts-node
import * as fs from "fs";
import { parse, stringify, transform } from "csv";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { convertGermanNumberToNumber, convertHyphenToNull } from "./helpers";

void yargs(hideBin(process.argv))
  .command(
    "buderus",
    "transform buderus export to comma csv for flux",
    (yargs) => {
      return yargs
        .option("input", {
          alias: "i",
          default: "files/data.csv",
        })
        .option("output", {
          alias: "o",
          default: "output.csv",
        });
    },
    async (args) => {
      console.log(`Input File: ${args.input} \nOutput File: ${args.output}`);
      const writableStream = fs.createWriteStream(args.output);
      fs.createReadStream(args.input)
        .pipe(
          parse({
            delimiter: ";",
            ignore_last_delimiters: true,
            skipEmptyLines: true,
            trim: true,
            columns: true,
          }),
        )
        .pipe(
          transform<Record<string, string>>((record) => {
            const keys = Object.keys(record);
            const item = structuredClone(record);

            for (let i = 0; i < keys.length; i++) {
              const cell = item[keys[i]];

              // Remove Category column
              if (keys[i] === "category") {
                // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                delete record[keys[i]];
              } else if (keys[i] === "timestamp") {
                // Convert time String to
                // @ts-ignore
                record[keys[i]] = new Date(cell);
              } else {
                const cellHyphen = convertHyphenToNull(cell);
                const cellPoint = convertGermanNumberToNumber(cellHyphen);

                record[keys[i]] = cellPoint as string;
              }
            }

            return record;
          }),
        )
        .pipe(
          stringify({
            quoted: true,
            header: true,
          }),
        )
        .pipe(writableStream);
    },
  )
  .command(
    "annotate",
    "transform csv to annotated flux format",
    (yargs) => {
      return yargs
        .option("input", {
          alias: "i",
          default: "files/data.csv",
        })
        .option("output", {
          alias: "o",
          default: "output.csv",
        });
    },
    async (args) => {
      console.log(`Input File: ${args.input} \nOutput File: ${args.output}`);
      const writableStream = fs.createWriteStream(args.output);
      const stringifier = stringify({
        header: true,
        columns: [
          "",
          "result",
          "table",
          "_time",
          "_value",
          "_field",
          "_measurement",
        ],
      });

      fs.createReadStream(args.input)
        .pipe(
          parse({
            trim: true,
            columns: true,
          }),
        )
        .pipe(
          transform<Record<string, string>>((record) => {
            const keys = Object.keys(record);
            for (let i = 0; i < keys.length; i++) {
              const key = keys[i];
              const cell = record[key];

              if (key === "timestamp") continue;

              stringifier.write({
                "": "",
                result: "",
                table: 0,
                _time: record.timestamp,
                _value: cell,
                _field: "value",
                _measurement: key,
              });
            }
          }),
        );

      writableStream.write("#group,false,false,true,true,true,true\n");
      writableStream.write(
        "#datatype,string,long,dateTime:RFC3339,double,string,string\n",
      );
      writableStream.write("#default,mean,,,,,\n");
      stringifier.pipe(writableStream);
    },
  )
  .demandCommand(1)
  .help()
  .parse();
