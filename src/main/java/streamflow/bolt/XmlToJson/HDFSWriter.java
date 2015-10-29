/*
 * Copyright MapR Technologies, $year
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streamflow.bolt.XmlToJson;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.google.inject.Inject;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import streamflow.annotations.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import streamflow.model.TopologyConnector;

import java.io.IOException;
import java.util.Map;
import com.google.inject.name.Named;
/**
 * Created by mpierre on 15-10-21.
 */

@Component(label = "HDFS Writer Bold", name = "HDFSWriter-bolt", type = "storm-bolt", icon="")
@Description("Writes stream to HDFS")
@ComponentOutputs({@ComponentInterface(key = "default", description = "any")})

public class HDFSWriter extends HdfsBolt {


    private String Separator;

    private RecordFormat format;
    private int syncCnt;
    private float fileRotationPolicy;
    private String pathName;
    private String fsUrl;
    private Configuration config;



    public int getSyncCnt() {
        return syncCnt;
    }

    @ComponentProperty(label = "Sync Policy Count", name = "SyncPolicyCnt", required = true, type = "int", defaultValue ="1000")
    @Description("Number of tuples before syncing with HDFS")
    @Inject
    public void setSyncCnt(@Named("SyncPolicyCnt") int syncCnt) {
        this.syncCnt = syncCnt;
        SyncPolicy syncP = new CountSyncPolicy(syncCnt);
        this.withSyncPolicy(syncP);
    }



    @ComponentProperty(label = "File Rotation Policy", name = "FileRotationPolicy", required = true, type = "float", defaultValue ="5.0")
    @Description("Number of mb to write to each file")
    @Inject
    public void setFileRotationPolicy(@Named("FileRotationPolicy") float fileRotationPolicy) {
        this.fileRotationPolicy = fileRotationPolicy;
        FileRotationPolicy policy = new FileSizeRotationPolicy(fileRotationPolicy, FileSizeRotationPolicy.Units.MB);
        this.withRotationPolicy(policy);
    }




    @ComponentProperty(label = "Path", name = "Path", required = true, type = "text", defaultValue ="/data/storm")
    @Description("Path to where the files will be written")
    @Inject
    public void setPathName(@Named("Path") String pathName) {
        this.pathName = pathName;
        FileNameFormat ff = new DefaultFileNameFormat().withPath(pathName);
        this.withFileNameFormat(ff);
    }

    @ComponentProperty(label = "Hdfs URL:(maprfs:///)", name = "HdfsUrl", required = true, type = "text", defaultValue ="maprfs:///")
    @Description("Url to the filesystem")
    @Inject
    public void setFsUrl(@Named("HdfsUrl") String fsUrl) {
        this.fsUrl = fsUrl;
        this.withFsUrl(fsUrl);
    }

    @ComponentProperty(label = "FieldSeparator", name = "FieldSeparator", required = true, type = "text", defaultValue ="|")
    @Description("Field Separator")
    @Inject
    public void setSeparator(@Named("FieldSeparator") String separator) {
        Separator = separator;

        format = new DelimitedRecordFormat().withFieldDelimiter(separator);
        this.withRecordFormat(format);
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);
    }

    public HDFSWriter() {
        super();

    }

    @Override
    public HdfsBolt withFsUrl(String fsUrl) {
        return super.withFsUrl(fsUrl);
    }

    @Override
    public HdfsBolt withFileNameFormat(FileNameFormat fileNameFormat) {
        return super.withFileNameFormat(fileNameFormat);
    }

    @Override
    public HdfsBolt withConfigKey(String configKey) {
        return super.withConfigKey(configKey);
    }

    @Override
    public HdfsBolt withRecordFormat(RecordFormat format) {
        return super.withRecordFormat(format);
    }

    @Override
    public HdfsBolt withSyncPolicy(SyncPolicy syncPolicy) {
        return super.withSyncPolicy(syncPolicy);
    }

    @Override
    public HdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
        return super.withRotationPolicy(rotationPolicy);
    }

    @Override
    public HdfsBolt addRotationAction(RotationAction action) {
        return super.addRotationAction(action);
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        config = new Configuration();
        config.addResource(new Path("/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/hdfs-site.xml"));
        conf.put("hdfs.config", conf);
        super.doPrepare(conf, topologyContext, collector);


    }
}
