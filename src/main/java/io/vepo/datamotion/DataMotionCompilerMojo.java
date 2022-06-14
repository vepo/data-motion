package io.vepo.datamotion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.maven.model.Resource;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "data-motion-compiler", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class DataMotionCompilerMojo extends AbstractMojo {

    public static final String FILENAME_STREAMER_YAML = "streamer.yaml";
    private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    MavenProject project;

    public void execute() throws MojoExecutionException, MojoFailureException {
        if (!project.getResources().isEmpty()) {
            Optional<Path> maybeConfig = ((List<Resource>) project.getResources()).stream()
                                                                                  .map(res -> Paths.get(res.getDirectory()))
                                                                                  .map(res -> res.resolve(FILENAME_STREAMER_YAML))
                                                                                  .filter(res -> res.toFile()
                                                                                                    .exists())

                                                                                  .findFirst();
            if (maybeConfig.isPresent()) {
                try {
                    StreamerDefinition def = mapper.readValue(Files.readAllBytes(maybeConfig.get()), StreamerDefinition.class);
                    StreamerGenerator generator = StreamerGenerator.from(project, def);
                    generator.generate();
                    getLog().info(String.format("Streamer configuration: %s", def));
                } catch (IOException e) {
                    getLog().error("Could not read YAML: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            } else {
                getLog().error(String.format("No %s found!", FILENAME_STREAMER_YAML));
            }
        } else {
            getLog().error("No resource folder found!");
        }
    }
}