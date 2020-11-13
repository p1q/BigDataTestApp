<h1 align="center">Big Data Test Application</h1>

<p>This is a very basic functional e-commerce application, written in Java using Servlets and JSPs. It complies fully with the MVC design pattern, as Servlets are used as Controllers and JSPs take care of the Presentation (View). The Model is described by Entities, that are mapped on tables in a MySQL relational database. The application has 2 versions of DAO layer implementation: plain JDBC and Hibernate.</p>
<p>You are free to use it as a template for your own online shop software. In current implementation you can add, remove and modify goods, users, roles and orders. The application provides registration and login procedures, passwords are saved into the database as hashes to increase security. You can make orders by adding goods into user's bucket.</p>

## :nut_and_bolt: Tech Stack
- Java 11
- Spring Boot 2
- Google Cloud Platform
- Maven

## :rocket: Project Deployment
1. Install JDK 11 and set correct JAVA_HOME variable
2. Install Git (distributed version control system)
3. Install an IDE
4. Clone project from GitHub into your IDE as a maven project.
5. Change GCP's credentials to yours.
6. That's all! You can run it.

## :man: Author

👤 **Eugeny Prokop**

- LinkedIn: [@EugenyProkop](https://www.linkedin.com/in/eugeny-prokop)
- GitHub: [@EugenyProkop](https://github.com/p1q)

## :scroll: License

Copyright © 2019 [Eugeny Prokop](https://github.com/p1q).<br />
This project is MIT licensed. See the [License](https://github.com/p1q/BigDataTestApp/blob/master/LICENSE) file.
