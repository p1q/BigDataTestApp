package com.prokop.app.data.big.test.bigdatatest.util;

import com.prokop.app.data.big.test.bigdatatest.model.Client;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public final class AvroUtility {
    private static final Logger LOGGER = Logger.getLogger(AvroUtility.class.getName());

    private AvroUtility() {
    }

    public static List<Client> deserializeAvro(byte[] avroBinary) {
        DatumReader<Client> userDatumReader = new SpecificDatumReader<>(Client.class);
        DataFileReader<Client> dataFileReader;
        List<Client> clients = new ArrayList<>();

        try {
            dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(avroBinary), userDatumReader);
            while (dataFileReader.hasNext()) {
                clients.add(dataFileReader.next());
            }
        } catch (IOException e) {
            LOGGER.severe("Input/Output error: " + e.toString());
        }
        return clients;
    }

    public static byte[] serializeNonOptionalAvro(List<Client> clients) {





        return null;
    }


}
