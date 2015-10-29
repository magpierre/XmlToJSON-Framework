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


package streamflow.util;




        import java.io.File;
        import java.io.FileInputStream;
        import java.io.FileNotFoundException;
        import java.io.FilenameFilter;
        import java.io.IOException;
        import java.io.InputStream;
        import java.io.Serializable;
        import java.util.Queue;
        import java.util.Set;
        import java.util.regex.Pattern;

        import com.google.common.base.Preconditions;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import com.google.common.collect.Lists;
        import com.google.common.collect.Queues;
        import com.google.common.collect.Sets;




/*

    This file is derived from: https://github.com/tdunning/mapr-spout
    Where I have simplified it for my simple use-case.

 */

public class DirectoryScanner implements Serializable {

    private static final long serialVersionUID = -8743538912863486122L;

    private Logger log = LoggerFactory.getLogger(DirectoryScanner.class);

    private final File inputDirectory;
    private final Pattern fileNamePattern;

    private Set<File> oldFiles = Sets.newHashSet();
    private Queue<File> pendingFiles = Queues.newConcurrentLinkedQueue();
    private InputStream liveInput = null;
    private File liveFile;

    public DirectoryScanner(File inputDirectory, Pattern fileNamePattern) {
        Preconditions.checkArgument(inputDirectory.exists(), String.format("Directory %s should already exist", inputDirectory));
        this.inputDirectory = inputDirectory;
        this.fileNamePattern = fileNamePattern;
    }

    private File scanForFiles() {
        if (pendingFiles.size() == 0) {
            Set<File> files = Sets.newTreeSet(Lists.newArrayList(inputDirectory.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File file, String s) {
                    return fileNamePattern.matcher(s).matches();
                }
            })));
            oldFiles.retainAll(files);
            files.removeAll(oldFiles);
            oldFiles.addAll(files);

            pendingFiles.addAll(files);
        }

        return pendingFiles.poll();
    }

    public File nextInput() {
        FileInputStream r;

        File nextInLine = scanForFiles();
        if (nextInLine == null) {
            log.trace("No new files");
            r = null;
        } else {

                return nextInLine;
        }



        return nextInLine;
    }

    public File getLiveFile() {
        return liveFile;
    }

    public Set<File> getOldFiles() {
        return oldFiles;
    }

    public File getInputDirectory() {
        return inputDirectory;
    }

    public Pattern getFileNamePattern() {
        return fileNamePattern;
    }

    public void setOldFiles(Set<File> oldFiles) {
        this.oldFiles = oldFiles;
    }

    public FileInputStream forceInput(File file, long offset) {
        FileInputStream r = null;
        try {
            liveFile = file;
            r = new FileInputStream(liveFile);
            r.getChannel().position(offset);
            liveInput = r;
        } catch (IOException e) {
            log.warn("Couldn't open replay file", e);
        }
        return r;
    }
}
