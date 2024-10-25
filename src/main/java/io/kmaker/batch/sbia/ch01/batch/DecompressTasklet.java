package io.kmaker.batch.sbia.ch01.batch;

import lombok.Setter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.ZipInputStream;

@Setter
public class DecompressTasklet implements Tasklet {

    private ResourceLoader resourceLoader;

    @Override
    public RepeatStatus execute(StepContribution contribution,
                                ChunkContext chunkContext) throws Exception {
        JobParameters jobParameters = contribution.getStepExecution().getJobParameters();
        Resource inputResource = resourceLoader.getResource(jobParameters.getString("inputResource"));
        String targetFile = jobParameters.getString("targetFile");
        String targetDirectory = jobParameters.getString("targetDirectory")
                ;
        ZipInputStream zis = new ZipInputStream(new BufferedInputStream(inputResource.getInputStream()));

        File targetDirectoryAsFile = new File(targetDirectory);
        if (!targetDirectoryAsFile.exists()) {
            FileUtils.forceMkdir(targetDirectoryAsFile);
        }

        File target = new File(targetDirectory, targetFile);

        BufferedOutputStream dest;
        while (zis.getNextEntry() != null) {
            if (!target.exists()) {
                target.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(target);
            dest = new BufferedOutputStream(fos);
            IOUtils.copy(zis, dest);
            dest.flush();
            dest.close();
        }
        zis.close();

        if (!target.exists()) {
            throw new IllegalStateException("Could not decompress anything from the archive!");
        }

        return RepeatStatus.FINISHED;
    }
}
