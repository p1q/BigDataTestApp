package com.prokop.app.data.big.test.bigdatatest.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public final class AvroUtility {
    private static final Logger LOGGER = Logger.getLogger(AvroUtility.class.getName());

    private AvroUtility() {
    }

    public static String binaryAvroToString(byte[] binaryAvro) {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();

        try (InputStream input = new ByteArrayInputStream(binaryAvro);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             DataFileStream<GenericRecord> streamReader = new DataFileStream<>(input, reader)) {

            Schema schema = streamReader.getSchema();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream, false);

            for (GenericRecord datum : streamReader) {
                writer.write(datum, jsonEncoder);
            }

            jsonEncoder.flush();
            outputStream.flush();
            return new String(outputStream.toByteArray());

        } catch (IOException e) {
            LOGGER.severe("Input/Output error: " + e.toString());
        }
        return "";
    }

    public static void loadAvroFromGCSToBQ(String avroUri, String datasetName, String tableName) {
        try {
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, avroUri, FormatOptions.avro());

            Job job = bigQuery.create(JobInfo.of(loadConfig));
            job = job.waitFor();

            if (job.isDone()) {
                LOGGER.info("Avro file was successfully saved to the table " + tableName);
            } else {
                LOGGER.severe("Error while loading to BigQuery: " + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            LOGGER.severe("Column not added during load append: " + e.toString());
        }
    }

    public static void writeBytesToBQ(byte[] binaryAvro, String datasetName, String tableName) {
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
        TableId tableId = TableId.of(datasetName, tableName);
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.avro())
                        .build();
        TableDataWriteChannel writer = bigQuery.writer(writeChannelConfiguration);

        try (OutputStream stream = Channels.newOutputStream(writer)) {
            stream.write(binaryAvro);
        } catch (IOException e) {
            LOGGER.severe("Input/Output error: " + e.toString());
        }

        Job job = writer.getJob();
        try {
            job = job.waitFor();
        } catch (InterruptedException e) {
            LOGGER.severe("Current thread was interrupted while waiting for the job to complete: "
                    + e.toString());
        }

        if (job.isDone()) {
            LOGGER.info("Avro file was successfully saved to the table " + tableName);
        } else {
            LOGGER.severe("Error while loading to BigQuery: " + job.getStatus().getError());
        }
    }

    public static String removeOptionalFieldsFromAvro(String stringAvro) {
        stringAvro = stringAvro.replaceAll("\\s+", "");
        List<Integer> idIndices = getSubstringIndices(stringAvro, "\"id\"");
        List<Integer> nameIndices = getSubstringIndices(stringAvro, "\"name\"");

        StringBuilder avroMandatory = new StringBuilder("{");
        for (int i = 0; i < idIndices.size(); i++) {
            avroMandatory.append("\"id\": ").append(getFieldValue(stringAvro, idIndices.get(i))).append(", ");
            avroMandatory.append("\"name\": ").append(getFieldValue(stringAvro, nameIndices.get(i)));
        }
        avroMandatory.append("}");
        return avroMandatory.toString();
    }

    public static byte[] encodeAvro(String json, String schemaString) {
        Schema schema = new Schema.Parser().parse(schemaString);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

        try (InputStream input = new ByteArrayInputStream(json.getBytes())) {
            DataInputStream dataInputStream = new DataInputStream(input);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);
            GenericRecord datum;

            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.write(datum, encoder);
            }

            encoder.flush();
            return output.toByteArray();
        } catch (IOException e) {
            LOGGER.severe("Input/Output error: " + e.toString());
        }
        return new byte[0];
    }

    private static List<Integer> getSubstringIndices(String string, String substring) {
        List<Integer> indices = new ArrayList<>();
        int index = string.indexOf(substring);
        indices.add(index);

        while (index >= 0) {
            index = string.indexOf(substring, index + 1);
            if (index > 0) {
                indices.add(index);
            }
        }
        return indices;
    }

    private static String getFieldValue(String string, int startIndex) {
        int endIndex = string.indexOf(',', startIndex) < 0
                || string.indexOf(',', startIndex) > string.indexOf('}', startIndex)
                ? string.indexOf('}', startIndex)
                : string.indexOf(',', startIndex);
        return string.substring(string.indexOf(':') + 1, endIndex);
    }
}
