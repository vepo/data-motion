package io.vepo.datamotion;

import org.apache.maven.archiver.ManifestConfiguration;
import org.apache.maven.archiver.MavenArchiveConfiguration;
import org.apache.maven.archiver.MavenArchiver;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.apache.maven.shared.model.fileset.FileSet;
import org.apache.maven.shared.model.fileset.util.FileSetManager;
import org.codehaus.plexus.archiver.Archiver;
import org.codehaus.plexus.archiver.jar.JarArchiver;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import static io.vepo.datamotion.StreamerConfiguration.findStreamerConfiguration;

@Mojo(name = "data-motion-packager", defaultPhase = LifecyclePhase.PACKAGE)
public class JarMojo extends AbstractMojo {
    private static final String[] DEFAULT_EXCLUDES = new String[]{"**/package.html"};

    private static final String[] DEFAULT_INCLUDES = new String[]{"**/**"};

    private static final String MODULE_DESCRIPTOR_FILE_NAME = "module-info.class";

    /**
     * List of files to include. Specified as fileset patterns which are relative to the input directory whose contents
     * is being packaged into the JAR.
     */
    @Parameter
    private String[] includes;

    /**
     * List of files to exclude. Specified as fileset patterns which are relative to the input directory whose contents
     * is being packaged into the JAR.
     */
    @Parameter
    private String[] excludes;

    /**
     * Directory containing the generated JAR.
     */
    @Parameter(defaultValue = "${project.build.directory}", required = true)
    private File outputDirectory;

    /**
     * Name of the generated JAR.
     */
    @Parameter(defaultValue = "${project.build.finalName}", readonly = true)
    private String finalName;

    /**
     * The Jar archiver.
     */
    @Component
    private Map<String, Archiver> archivers;

    /**
     * The {@link {MavenProject}.
     */
    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    /**
     * The {@link MavenSession}.
     */
    @Parameter(defaultValue = "${session}", readonly = true, required = true)
    private MavenSession session;

    /**
     * The archive configuration to use. See <a href="http://maven.apache.org/shared/maven-archiver/index.html">Maven
     * Archiver Reference</a>.
     */
    @Parameter
    private MavenArchiveConfiguration archive = new MavenArchiveConfiguration();

    /**
     *
     */
    @Component
    private MavenProjectHelper projectHelper;

    /**
     * Require the jar plugin to build a new JAR even if none of the contents appear to have changed. By default, this
     * plugin looks to see if the output jar exists and inputs have not changed. If these conditions are true, the
     * plugin skips creation of the jar. This does not work when other plugins, like the maven-shade-plugin, are
     * configured to post-process the jar. This plugin can not detect the post-processing, and so leaves the
     * post-processed jar in place. This can lead to failures when those plugins do not expect to find their own output
     * as an input. Set this parameter to <tt>true</tt> to avoid these problems by forcing this plugin to recreate the
     * jar every time.<br/>
     * Starting with <b>3.0.0</b> the property has been renamed from <code>jar.forceCreation</code> to
     * <code>maven.jar.forceCreation</code>.
     */
    @Parameter(property = "maven.jar.forceCreation", defaultValue = "false")
    private boolean forceCreation;

    /**
     * Skip creating empty archives.
     */
    @Parameter(defaultValue = "false")
    private boolean skipIfEmpty;

    /**
     * Timestamp for reproducible output archive entries, either formatted as ISO 8601
     * <code>yyyy-MM-dd'T'HH:mm:ssXXX</code> or as an int representing seconds since the epoch (like
     * <a href="https://reproducible-builds.org/docs/source-date-epoch/">SOURCE_DATE_EPOCH</a>).
     *
     * @since 3.2.0
     */
    @Parameter(defaultValue = "${project.build.outputTimestamp}")
    private String outputTimestamp;

    /**
     * Directory containing the classes and resource files that should be packaged into the JAR.
     */
    @Parameter(defaultValue = "${project.build.outputDirectory}", required = true)
    private File classesDirectory;

    /**
     * @return the {@link #project}
     */
    protected final MavenProject getProject() {
        return project;
    }

    /**
     * Classifier to add to the artifact generated. If given, the artifact will be attached
     * as a supplemental artifact.
     * If not given this will create the main artifact which is the default behavior.
     * If you try to do that a second time without using a classifier the build will fail.
     */
    @Parameter
    private String classifier;
    private String type = "jar";

    /**
     * Returns the Jar file to generate, based on an optional classifier.
     *
     * @param basedir         the output directory
     * @param resultFinalName the name of the ear file
     * @param classifier      an optional classifier
     * @return the file to generate
     */
    protected File getJarFile(File basedir, String resultFinalName, String classifier) {
        if (basedir == null) {
            throw new IllegalArgumentException("basedir is not allowed to be null");
        }
        if (resultFinalName == null) {
            throw new IllegalArgumentException("finalName is not allowed to be null");
        }

        StringBuilder fileName = new StringBuilder(resultFinalName);

        if (hasClassifier()) {
            fileName.append("-").append(classifier);
        }

        fileName.append(".jar");

        return new File(basedir, fileName.toString());
    }

    /**
     * Generates the JAR.
     *
     * @return The instance of File for the created archive file.
     * @throws MojoExecutionException in case of an error.
     */
    public File createArchive() throws MojoExecutionException {
        return findStreamerConfiguration(project, getLog())
                .map(config -> {
                    File jarFile = getJarFile(outputDirectory, finalName, classifier);

                    FileSetManager fileSetManager = new FileSetManager();
                    FileSet jarContentFileSet = new FileSet();
                    jarContentFileSet.setDirectory(classesDirectory.getAbsolutePath());
                    jarContentFileSet.setIncludes(Arrays.asList(getIncludes()));
                    jarContentFileSet.setExcludes(Arrays.asList(getExcludes()));

                    boolean containsModuleDescriptor = false;
                    String[] includedFiles = fileSetManager.getIncludedFiles(jarContentFileSet);
                    for (String includedFile : includedFiles) {
                        // May give false positives if the files is named as module descriptor
                        // but is not in the root of the archive or in the versioned area
                        // (and hence not actually a module descriptor).
                        // That is fine since the modular Jar archiver will gracefully
                        // handle such case.
                        // And also such case is unlikely to happen as file ending
                        // with "module-info.class" is unlikely to be included in Jar file
                        // unless it is a module descriptor.
                        if (includedFile.endsWith(MODULE_DESCRIPTOR_FILE_NAME)) {
                            containsModuleDescriptor = true;
                            break;
                        }
                    }

                    MavenArchiver archiver = new MavenArchiver();
                    archiver.setCreatedBy("Maven JAR Plugin", "org.apache.maven.plugins", "maven-jar-plugin");

                    if (containsModuleDescriptor) {
                        archiver.setArchiver((JarArchiver) archivers.get("mjar"));
                    } else {
                        archiver.setArchiver((JarArchiver) archivers.get("jar"));
                    }

                    archiver.setOutputFile(jarFile);

                    // configure for Reproducible Builds based on outputTimestamp value
                    archiver.configureReproducible(outputTimestamp);

                    archive.setForced(forceCreation);
                    ManifestConfiguration manifest = new ManifestConfiguration();
                    manifest.setMainClass(config.getMainClassFullQualifiedName());
                    archive.setManifest(manifest);
                    try {
                        if (!classesDirectory.exists()) {
                            if (!forceCreation) {
                                getLog().warn("JAR will be empty - no content was marked for inclusion!");
                            }
                        } else {
                            archiver.getArchiver().addDirectory(classesDirectory, getIncludes(), getExcludes());
                        }

                        archiver.createArchive(session, project, archive);

                        return jarFile;
                    } catch (Exception e) {
                        getLog().error("Error assembling JAR!", e);
                        return null;
                    }
                })
                .orElseThrow(() -> new MojoExecutionException("Error assembling JAR"));
    }

    /**
     * Generates the JAR.
     *
     * @throws MojoExecutionException in case of an error.
     */
    public void execute() throws MojoExecutionException {
        if (skipIfEmpty && (!classesDirectory.exists() || classesDirectory.list().length < 1)) {
            getLog().info("Skipping packaging of the " + type);
        } else {
            File jarFile = createArchive();

            if (hasClassifier()) {
                projectHelper.attachArtifact(getProject(), type, classifier, jarFile);
            } else {
                if (projectHasAlreadySetAnArtifact()) {
                    throw new MojoExecutionException("You have to use a classifier "
                                                             + "to attach supplemental artifacts to the project instead of replacing them.");
                }
                getProject().getArtifact().setFile(jarFile);
            }
        }
    }

    private boolean projectHasAlreadySetAnArtifact() {
        if (getProject().getArtifact().getFile() != null) {
            return getProject().getArtifact().getFile().isFile();
        } else {
            return false;
        }
    }

    /**
     * @return true in case where the classifier is not {@code null} and contains something else than white spaces.
     */
    protected boolean hasClassifier() {
        boolean result = false;
        if (classifier != null && classifier.trim().length() > 0) {
            result = true;
        }

        return result;
    }

    private String[] getIncludes() {
        if (includes != null && includes.length > 0) {
            return includes;
        }
        return DEFAULT_INCLUDES;
    }

    private String[] getExcludes() {
        if (excludes != null && excludes.length > 0) {
            return excludes;
        }
        return DEFAULT_EXCLUDES;
    }
}