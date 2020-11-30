package com.prokop.app.data.big.test.bigdatatest.repository.impl;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.prokop.app.data.big.test.bigdatatest.exception.ReadFileFromCloudStorageException;
import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.repository.CloudStorageRepository;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest()
@RunWith(SpringRunner.class)
public class CloudStorageRepositoryImplTest {
    private static final String BUCKET_NAME = "test-bucket";
    private static final Long SIZE = 100L;
    private static final String CRC32C = "test-crc32c";
    private static final String URI = "test-uri";
    private static final Long GENERATION = 123456789L;
    private static final FileIdentifier FILE_IDENTIFIER =
            new FileIdentifier("src\\test\\resources\\test-file", "123456789");

    private final CloudStorageRepository cloudStorageRepository = new CloudStorageRepositoryImpl();

    @Mock
    Storage mockStorage;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testReadingFile() throws ReadFileFromCloudStorageException {
        final Blob blob = mock(Blob.class);

        when(blob.getBucket()).thenReturn(BUCKET_NAME);
        when(blob.getSize()).thenReturn(SIZE);
        when(blob.getCrc32c()).thenReturn(CRC32C);
        when(blob.getGeneration()).thenReturn(GENERATION);
        when(blob.getSelfLink()).thenReturn(URI);

        when(mockStorage.get(any(BlobId.class))).thenReturn(blob);

        assertEquals(blob.getBucket(), cloudStorageRepository
                .readObjectFromGCS(mockStorage, FILE_IDENTIFIER, BUCKET_NAME).getBucket());
        assertEquals(blob.getSize(), cloudStorageRepository
                .readObjectFromGCS(mockStorage, FILE_IDENTIFIER, BUCKET_NAME).getSize());
        assertEquals(blob.getCrc32c(), cloudStorageRepository
                .readObjectFromGCS(mockStorage, FILE_IDENTIFIER, BUCKET_NAME).getCrc32c());
        assertEquals(blob.getGeneration(), cloudStorageRepository
                .readObjectFromGCS(mockStorage, FILE_IDENTIFIER, BUCKET_NAME).getGeneration());
        assertEquals(blob.getSelfLink(), cloudStorageRepository
                .readObjectFromGCS(mockStorage, FILE_IDENTIFIER, BUCKET_NAME).getSelfLink());

        verify(mockStorage, times(5)).get(any(BlobId.class));
    }

    @Test
    public void testStorageExceptionOnFetch() throws ReadFileFromCloudStorageException {
        given(cloudStorageRepository.readObjectFromGCS(mockStorage, FILE_IDENTIFIER, BUCKET_NAME))
                .willAnswer( invocation -> {
                    throw new ReadFileFromCloudStorageException("Error reading file.");
                });
    }
}
