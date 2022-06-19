package io.vepo.datamotion;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.Resource;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.eclipse.sisu.Parameters;

import javax.inject.Inject;

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