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

package  streamflow.spout.XmlToJson;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import streamflow.util.DirectoryScanner;
import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import streamflow.annotations.Component;
import streamflow.annotations.ComponentOutputs;
import streamflow.annotations.ComponentProperty;
import streamflow.annotations.Description;
import streamflow.annotations.ComponentInterface;

/**
 * Created by mpierre on 15-10-14.
 */

@Component(label = "Path Browser Spout", name = "Path-Browser-spout", type = "storm-spout", icon="")
@Description("Finds new files in a directory")
@ComponentOutputs({@ComponentInterface(key = "default", description = "Names of files.")})
public class PathBrowserSpout  extends BaseRichSpout {

    private static final long serialVersionUID = -325235325235L;
    private Logger LOG;
    SpoutOutputCollector _collector;

    private String filePattern;

    public String getInputPath() {
        return inputPath;
    }

    public String getFilePattern() {
        return filePattern;
    }

    @ComponentProperty(label = "File pattern of files to track", name = "filePattern", required = true, type = "text", defaultValue = ".*xml")
    @Description("File Pattern of files to track")
    @Inject
    public void setFilePattern(@Named("filePattern") String filePattern) {
        this.filePattern = filePattern;
    }

    @ComponentProperty(label = "Folder to track", name = "inputPath", required = true, type = "text", defaultValue = "/mapr/demo.mapr.com/data/landing")
    @Description("Posix folder to track")
    @Inject
    public void setInputPath(@Named("inputPath") String inputPath) {
        this.inputPath = inputPath;
    }

    private String inputPath;

    DirectoryScanner myScanner;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Deliver the paths
        declarer.declare(true,new Fields("filePath"));

    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        _collector = collector;
        File mydir = new File(getInputPath());

        if(mydir != null && mydir.canRead() == true) {
            myScanner = new DirectoryScanner(mydir,Pattern.compile(getFilePattern()));
        }
        else {
            LOG.debug("Could not open directory");
        }
    }

    @Override
    public void nextTuple() {
        File retFile = myScanner.nextInput();
        if(retFile == null)
            return;

        LOG.info(new String("File found: "+retFile.getAbsolutePath()));

        _collector.emit(new Values(retFile),retFile.getAbsolutePath());

    }

    @Inject
    public void setLogger(Logger logger){
        this.LOG = logger;
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        LOG.info(new String((String) msgId) + "is processed" );

    }
}
