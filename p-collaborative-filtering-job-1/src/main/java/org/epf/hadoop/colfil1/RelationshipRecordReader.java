package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Lire la prochaine ligne avec LineRecordReader
        boolean hasNext = lineRecordReader.nextKeyValue();
        if (hasNext) {
            // Lire le numéro de la ligne (key)
            currentKey.set(lineRecordReader.getCurrentKey().get());

            // Lire la ligne actuelle (valeur brute)
            String line = lineRecordReader.getCurrentValue().toString();

            // Parse la ligne pour extraire id1 et id2
            String[] parts = line.split(","); // Séparer "id1<->id2" et "timestamp"
            if (parts.length >= 1) {
                String relationshipPart = parts[0]; // Partie "id1<->id2"
                String[] ids = relationshipPart.split("<->"); // Séparer id1 et id2
                if (ids.length == 2) {
                    currentValue.setId1(ids[0]);
                    currentValue.setId2(ids[1]);
                }
            }
        }
        return hasNext;
    }


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Relationship getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
