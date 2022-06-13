package io.vepo.datamotion;

import java.util.List;

import org.apache.maven.model.Dependency;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "data-motion-compiler", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class DataMotionCompilerMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    MavenProject project;



    public void execute() throws MojoExecutionException, MojoFailureException {
        List<Dependency> dependencies = project.getDependencies();
        long numDependencies = dependencies.stream().count();
        getLog().info("Number of dependencies: " + numDependencies);
    }
}
