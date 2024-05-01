if [ "$#" -ne 3 ]; then
	echo "Usage: $0 <first_arg> <second_arg> <third_arg>"
	exit 1
fi

# Compile Java files
bin/hadoop com.sun.tools.javac.Main "$1.java"

# Create a JAR file
jar cf "$1.jar" "$1"*.class

# Execute Hadoop job
bin/hadoop jar "$1.jar" "$1" "$2" "$3"
