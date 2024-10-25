package io.kmaker.batch.service;

import org.springframework.stereotype.Service;

@Service
public class FileService {
    private int pollCount = 0;

    // Simulate the file being ready after 3 polls
    public boolean isFileReady() {
        pollCount++;
        return pollCount >= 3;  // After 3 polls, the file is ready
    }
}
