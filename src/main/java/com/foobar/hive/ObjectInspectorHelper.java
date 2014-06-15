package com.foobar.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ObjectInspectorHelper {
    private List<ObjectInspector> fieldObjectInspectors;
    private List<String> fieldNames;

    public StandardStructObjectInspector getObjectInspector() {
        return objectInspector;
    }

    private StandardStructObjectInspector objectInspector;

    private void setSchema(RowSchema schema) {
        fieldNames = new ArrayList<String>();
        fieldObjectInspectors = new ArrayList<ObjectInspector>();
        for(Map.Entry<String, ? extends Class> entry : schema.entrySet())
        {
            fieldNames.add(entry.getKey());
            fieldObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(
                    entry.getValue(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        }
        objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
    }

    public ObjectInspectorHelper(RowSchema schema) {
        this.setSchema(schema);
    }

}
