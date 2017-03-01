If you get the following error when importing projects into Eclipse:

Missing artifact jdk.tools:jdk.tools:jar:1.7 pom.xml	Maven Dependency Problem

The problem is in the Eclipse Maven support. 

Under Eclipse, the java.home variable is set to the JRE that was used to start Eclipse, not the build JRE. The default system JRE from C:\Program Files doesn't include the JDK so tools.jar is not being found.

To fix the issue you need to start Eclipse using the JRE from the JDK by adding  this to eclipse.ini (before -vmargs!):

-vm
C:\<your_path_to_jdk170>\jre\bin\javaw.exe

Then refresh the Maven dependencies (Alt-F5) (Just refreshing the project isn't sufficient).

Another way to sove this same problem is by adding this to your pom.xml

		<dependency>
			<groupId>jdk.tools</groupId>
			<artifactId>jdk.tools</artifactId>
			<version>1.7</version>
			<scope>system</scope>
			<systemPath>C:\<your_path_to_jdk170>\lib\tools.jar</systemPath>
		</dependency>