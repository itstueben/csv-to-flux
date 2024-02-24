#!/usr/bin/env ts-node
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { parse, stringify, transform } from "csv";
import * as fs from "fs";
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
                record[keys[i]] = new Date(cell).toISOString();
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
          alias: "if",
          default: "files/data.csv",
        })
        .option("output", {
          alias: "of",
          default: "output.csv",
        });
    },
    async (args) => {
      console.log(`Input File: ${args.input} \nOutput File: ${args.output}`);
      const writableStream = fs.createWriteStream(args.output);

      const readStream = fs
        .createReadStream(args.input)
        .pipe(parse({ raw: true }))
        .pipe(
          transform((record) => {
            console.log(record.raw);

            return "," + record.raw;
          }),
        );
      // .pipe(writableStream);

      writableStream.write("fdsfds\n");
      readStream.pipe(writableStream);
    },
  )
  .demandCommand(1)
  .help()
  .parse();
