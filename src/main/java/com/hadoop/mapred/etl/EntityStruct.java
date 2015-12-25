package com.hadoop.mapred.etl;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Created by vaibhavguptabits on 22.09.15.
 */
public class EntityStruct {

    public String entityId;


    private List<DistanceStruct> distances;
    private List<DuplicateStruct> duplicates;


    public  List<String>  values;


    public class DistanceStruct {
        public List<DuplicateStruct> duplicates = Lists.newLinkedList();
        public String entityId;
        public Double addressDistance;
        public String address;
        public String fileID;

    }



    public EntityStruct( String entityId, List<String> inputValues ) {
        distances = Lists.newLinkedList();
        //input values is a linked list containing the all the values of the keys , each value word is delimited by comma
        Iterator<String> itr = inputValues.iterator();

        values = Lists.newLinkedList();

        this.entityId =entityId;

        while (itr.hasNext()) {
            String next = itr.next();
            values.add(next);
        }

        }

    //here we are just converting entity struct to another object distance struct which has address as parameter and

    public List<DistanceStruct> getDistanceStructs () {

        for (String value : values) {
            String[] line = value.toString().split(",");
            DistanceStruct clusterBag = new DistanceStruct();
            clusterBag.entityId = entityId;
            clusterBag.fileID = line[0];
            StringBuffer sb = new StringBuffer();
            //start from 2 because index 0 contain fileId, 1 contain enitityId
            for (int i=2; i < line.length; i++) {
                if (i != 2) sb.append(" ");
                sb.append(line[i]);
            }
            clusterBag.address = sb.toString();

            distances.add(clusterBag);
        }
        return distances;
    }







    }

