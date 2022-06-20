package io.vepo.datamotion;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;

import static io.vepo.datamotion.StreamerConfiguration.findStreamerConfiguration;

@Mojo(name = "data-motion-compiler", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class DataMotionCompilerMojo extends AbstractMojo {


    @Parameter(defaultValue = "${project}", required = true, readonly = false)
    MavenProject project;

    /**
     * The directory for generated source files.
     */
    @Parameter(defaultValue = "${project.build.directory}/generated-sources")
    private File generatedSourcesDirectory;

    @Parameter(defaultValue = "${project.build.directory}/classes/")
    private File classesDirectory;

    public void execute() {
        findStreamerConfiguration(project, getLog()).ifPresent(config -> {
            StreamerGenerator generator = StreamerGenerator.from(classesDirectory, generatedSourcesDirectory, config);
            generator.generate(project);
        });
    }
}