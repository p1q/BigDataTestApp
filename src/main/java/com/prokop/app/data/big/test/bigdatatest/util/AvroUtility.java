package com.prokop.app.data.big.test.bigdatatest.util;

import com.prokop.app.data.big.test.bigdatatest.model.Client;
import com.prokop.app.data.big.test.bigdatatest.model.ClientMandatory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;

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
        DatumWriter<ClientMandatory> userDatumWriter = new SpecificDatumWriter<>(ClientMandatory.class);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        List<ClientMandatory> clientsMandatory = createClientsMandatoryFromClients(clients);

        try (DataFileWriter<ClientMandatory> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(clientsMandatory.get(0).getSchema(), byteArrayOutputStream);
            for (ClientMandatory clientMandatory : clientsMandatory) {
                dataFileWriter.append(clientMandatory);
            }
        } catch (IOException e) {
            LOGGER.severe("Input/Output error: " + e.toString());
        }

        return byteArrayOutputStream.toByteArray();
    }

    private static List<ClientMandatory> createClientsMandatoryFromClients(List<Client> clients) {
        List<ClientMandatory> clientsMandatory = new ArrayList<>();

        for (Client client : clients) {
            ClientMandatory clientMandatory = new ClientMandatory();
            clientMandatory.setId(client.getId());
            clientMandatory.setName(client.getName());
            clientMandatory.setGender(client.getGender());
            clientMandatory.setActive(client.getActive());
            clientMandatory.setLastPurchases(client.getLastPurchases());

            clientsMandatory.add(clientMandatory);
        }
        return clientsMandatory;
    }
}
