package com.hadoop.mapred;

import com.hadoop.mapred.etl.DuplicateStruct;
import com.hadoop.mapred.etl.EntityAnalysisETL;
import com.hadoop.mapred.etl.EntityStruct;
import com.google.common.collect.Lists;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by vaibhavguptabits on 22.09.15.
 */
public class RunClusterFlow {

    private static final String projectRootPath = System.getProperty("user.dir");
    private static final String mapped_data = "mapped_data";
    private static final String clustered_data = "clustered";

    public static void main(String[] args) {

        Path inputFile = null;
        if(EntityAnalysisMRJob.runOnCluster) {
            inputFile = Paths.get(projectRootPath, mapped_data);
        } else {
            inputFile = Paths.get(projectRootPath, "output");
        }


        Path outputFile = Paths.get(projectRootPath, clustered_data);

        List<EntityStruct> entities = EntityAnalysisETL.extractData(inputFile);


        List<EntityStruct.DistanceStruct> clusters =  EntityAnalysisETL.transformData(entities);

        List<String> duplicates = Lists.newLinkedList();

        for (EntityStruct.DistanceStruct cluster : clusters) {

            if(cluster.duplicates.size() > 0 ) {


                for (DuplicateStruct duplicate : cluster.duplicates) {

                    String[] addressLine = duplicate.value.split(" ");

                    if (addressLine.length > 1 ) {
                        duplicates.add(duplicate.toString());
                    }
                }

            }
        }


        EntityAnalysisETL.loadData(duplicates, outputFile);

    }
}
