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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.xml.sax.SAXException;
import streamflow.annotations.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by mpierre on 15-10-20.
 */
@Component(label = "Posix File writer bolt", name = "PosixFileWriter-bolt", type = "storm-bolt", icon="")
@Description("Write Files to regular file system")
@ComponentInputs({@ComponentInterface(key = "default", description = "any")})
public class FileWriterBolt  extends BaseRichBolt {

    private String outputFolder;
    private String outputExtension;
    private Logger LOG;

    private  OutputCollector _collector;
    public String getOutputFolder() {
        return outputFolder;
    }

    @ComponentProperty(label = "Folder to store generated files", name = "outputFolder", required = true, type = "text", defaultValue = "/mapr/demo.mapr.com/data/ingested")
    @Description("Folder of where to store generated files")
    @Inject
    public void setOutputFolder(@Named("outputFolder") String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public String getOutputExtension() {
        return outputExtension;
    }

    @ComponentProperty(label = "Extension of generated files", name = "outputExt", required = true, type = "text", defaultValue = ".json")
    @Description("Extension of generated files")
    @Inject
    public void setOutputExtension(@Named("outputExt") String outputExtension) {
        this.outputExtension = outputExtension;
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        _collector = outputCollector;
    }

    private static String extractFileName( String filePathName )
    {
        if ( filePathName == null )
            return null;

        int dotPos = filePathName.lastIndexOf( '.' );
        int slashPos = filePathName.lastIndexOf( '\\' );
        if ( slashPos == -1 )
            slashPos = filePathName.lastIndexOf( '/' );

        if ( dotPos > slashPos )
        {
            return filePathName.substring( slashPos > 0 ? slashPos + 1 : 0,
                    dotPos );
        }

        return filePathName.substring( slashPos > 0 ? slashPos + 1 : 0 );
    }

    @Override
    public void execute(Tuple input) {
        try {
            File p = (File) input.getValue(0);
            JSONObject o = (JSONObject) input.getValue(1);


            BufferedWriter output = null;

            String name = extractFileName(p.getAbsolutePath()).trim();
            String newName = getOutputFolder().trim() + "/" + name + getOutputExtension().trim();

            LOG.info("Storing data in file:" +newName);
            File file = new File(newName);
            output = new BufferedWriter(new FileWriter(file));
            o.writeJSONString(output);
            output.close();

            _collector.ack(input);

        } catch (IOException e) {
                LOG.error(e.getMessage());

        }
    }

    @Inject
    public void setLogger(Logger logger){
        this.LOG = logger;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
