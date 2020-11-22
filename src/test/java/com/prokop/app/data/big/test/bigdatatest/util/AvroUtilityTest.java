package com.prokop.app.data.big.test.bigdatatest.util;

import com.prokop.app.data.big.test.bigdatatest.model.Client;
import com.prokop.app.data.big.test.bigdatatest.model.ClientMandatory;
import com.prokop.app.data.big.test.bigdatatest.model.Gender;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class AvroUtilityTest {
    @Test
    public void testDeserializeAvro_ShouldReturnValidListOfClients() throws IOException {
        byte[] binaryAvro = Files.readAllBytes(Paths.get("src\\main\\resources\\avro-sample\\avro-sample.avro"));

        List<Client> expectedResult = getClientsList();
        List<Client> actualResult = AvroUtility.deserializeAvro(binaryAvro);

        assertEquals(expectedResult, actualResult);
    }

    @Test(expected = java.lang.AssertionError.class)
    public void testDeserializeAvro_ShouldReturnAssertionErrorWhenWrongData() throws IOException {
        byte[] binaryAvro = Files.readAllBytes(Paths.get("src\\main\\resources\\avro-sample\\avro-sample.avro"));

        List<Client> expectedResult = new ArrayList<>();
        expectedResult.add(new Client());
        List<Client> actualResult = AvroUtility.deserializeAvro(binaryAvro);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testSerializeNonOptionalAvro_ShouldReturnValidBinaryAvro_OfMandatoryFieldsOfClients() throws IOException {
        byte[] expectedResultByteArray = Files.readAllBytes(Paths.get("src\\main\\resources\\avro-sample\\avro-sample-mandatory.avro"));
        byte[] actualResultByteArray = AvroUtility.serializeNonOptionalAvro(getClientsList());

        List<ClientMandatory> expectedResult = deserializeAvroMandatory(expectedResultByteArray);
        List<ClientMandatory> actualResult = deserializeAvroMandatory(actualResultByteArray);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testBothSerializeAndDeserializeMethodsTogether_ShouldReturnValidBinaryAvro_OfMandatoryFieldsOfClients() throws IOException {
        byte[] binaryAvro = Files.readAllBytes(Paths.get("src\\main\\resources\\avro-sample\\avro-sample.avro"));
        byte[] actualBinaryAvroMandatory = Files.readAllBytes(Paths.get("src\\main\\resources\\avro-sample\\avro-sample-mandatory.avro"));

        List<Client> clientsList = AvroUtility.deserializeAvro(binaryAvro);
        byte[] expectedBinaryAvroMandatory = AvroUtility.serializeNonOptionalAvro(clientsList);

        List<ClientMandatory> expectedResult = deserializeAvroMandatory(expectedBinaryAvroMandatory);
        List<ClientMandatory> actualResult = deserializeAvroMandatory(actualBinaryAvroMandatory);

        assertEquals(expectedResult, actualResult);
    }

    private List<Client> getClientsList() {
        List<Client> clients = new ArrayList<>();
        clients.add(new Client(1L, "Serj Sergeeff", true, Gender.male, new ArrayList<>(List.of("Item1", "Item2", "Item3")), 39, "380682146587", null, 458.25));
        clients.add(new Client(2L, "Victor Smirnoff", true, Gender.male, new ArrayList<>(List.of("Item5", "Item6")), 26, null, null, 138.85));
        clients.add(new Client(3L, "Mykola Petroff", false, Gender.unknown, new ArrayList<>(List.of("Item7")), 21, "3809365482", "Odessa, Filatova str., 35", null));
        clients.add(new Client(4L, "Alex Ivanoff", false, Gender.unknown, new ArrayList<>(List.of("Item8", "Item9", "Item10")), 55, "380682146587", "Odessa, Generala Petrova str., 38", 54.30));
        clients.add(new Client(5L, "Alina Sudak", true, Gender.female, new ArrayList<>(List.of("Item11", "Item12", "Item13")), 33, "0963256487", null, 1547.05));
        return clients;
    }

    private static List<ClientMandatory> deserializeAvroMandatory(byte[] avroBinaryMandatory) {
        DatumReader<ClientMandatory> userDatumReader = new SpecificDatumReader<>(ClientMandatory.class);
        DataFileReader<ClientMandatory> dataFileReader;
        List<ClientMandatory> clients = new ArrayList<>();

        try {
            dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(avroBinaryMandatory), userDatumReader);
            while (dataFileReader.hasNext()) {
                clients.add(dataFileReader.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return clients;
    }
}
