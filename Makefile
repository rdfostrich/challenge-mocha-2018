default: build dockerize

build:
	./gradlew shadowJar

dockerize:
	docker build -t git.project-hobbit.eu:4567/ruben.taelman/mocha-3-ostrich .
	docker push git.project-hobbit.eu:4567/ruben.taelman/mocha-3-ostrich
