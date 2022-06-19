package io.vepo.datamotion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.text.CaseUtils;
import org.apache.maven.model.Resource;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.MavenProject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.apache.commons.text.CaseUtils.toCamelCase;

public class StreamerConfiguration {
    public static final String FILENAME_STREAMER_YAML = "streamer.yaml";
    private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private final StreamerDefinition definition;

    public StreamerConfiguration(StreamerDefinition definition) {
        this.definition = definition;
    }

    public StreamerDefinition getDefinition() {
        return definition;
    }

    public static Optional<StreamerConfiguration> findStreamerConfiguration(MavenProject project, Log log) {
        if (!project.getResources().isEmpty()) {
            Optional<Path> maybeConfig = ((List<Resource>) project.getResources()).stream()
                                                                                  .map(res -> Paths.get(res.getDirectory()))
                                                                                  .map(res -> res.resolve(FILENAME_STREAMER_YAML))
                                                                                  .filter(res -> res.toFile()
                                                                                                    .exists())

                                                                                  .findFirst();
            if (maybeConfig.isPresent()) {
                try {
                    return Optional.of(new StreamerConfiguration(mapper.readValue(Files.readAllBytes(maybeConfig.get()), StreamerDefinition.class)));
                } catch (IOException e) {
                    log.error("Error reading Streamer definition!", e);
                }
            } else {
                log.error(String.format("No %s found!", FILENAME_STREAMER_YAML));
            }
        } else {
            log.error("No resource folder found!");
        }
        return Optional.empty();
    }

    public String getMainClassFullQualifiedName() {
        return String.format("%s.%sStreamer", definition.getPackageName(), toCamelCase(definition.getId(),
                                                                                       true,
                                                                                       '_',
                                                                                       '-'));
    }
}
