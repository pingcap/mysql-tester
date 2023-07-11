DROP SCHEMA IF EXISTS sakila;
CREATE SCHEMA sakila;
USE sakila;
SET FOREIGN_KEY_CHECKS=0;




CREATE TABLE actor (
                       actor_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                       first_name VARCHAR(45) NOT NULL,
                       last_name VARCHAR(45) NOT NULL,
                       last_update TIMESTAMP NOT NULL ,
                       PRIMARY KEY  (actor_id),
                       KEY idx_actor_last_name (last_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE address (
                         address_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                         address VARCHAR(50) NOT NULL,
                         address2 VARCHAR(50) DEFAULT NULL,
                         district VARCHAR(20) NOT NULL,
                         city_id SMALLINT UNSIGNED NOT NULL,
                         postal_code VARCHAR(10) DEFAULT NULL,
                         phone VARCHAR(20) NOT NULL,


                         last_update TIMESTAMP NOT NULL ,
                         PRIMARY KEY  (address_id),
                         KEY idx_fk_city_id (city_id),
                         CONSTRAINT `fk_address_city` FOREIGN KEY (city_id) REFERENCES city (city_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE category (
                          category_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                          name VARCHAR(25) NOT NULL,
                          last_update TIMESTAMP NOT NULL ,
                          PRIMARY KEY  (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE city (
                      city_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                      city VARCHAR(50) NOT NULL,
                      country_id SMALLINT UNSIGNED NOT NULL,
                      last_update TIMESTAMP NOT NULL ,
                      PRIMARY KEY  (city_id),
                      KEY idx_fk_country_id (country_id),
                      CONSTRAINT `fk_city_country` FOREIGN KEY (country_id) REFERENCES country (country_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE country (
                         country_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                         country VARCHAR(50) NOT NULL,
                         last_update TIMESTAMP NOT NULL ,
                         PRIMARY KEY  (country_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE customer (
                          customer_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                          store_id TINYINT UNSIGNED NOT NULL,
                          first_name VARCHAR(45) NOT NULL,
                          last_name VARCHAR(45) NOT NULL,
                          email VARCHAR(50) DEFAULT NULL,
                          address_id SMALLINT UNSIGNED NOT NULL,
                          active BOOLEAN NOT NULL DEFAULT TRUE,
                          create_date DATETIME NOT NULL,
                          last_update TIMESTAMP ,
                          PRIMARY KEY  (customer_id),
                          KEY idx_fk_store_id (store_id),
                          KEY idx_fk_address_id (address_id),
                          KEY idx_last_name (last_name),
                          CONSTRAINT fk_customer_address FOREIGN KEY (address_id) REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                          CONSTRAINT fk_customer_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE film (
                      film_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                      title VARCHAR(128) NOT NULL,
                      description TEXT DEFAULT NULL,
                      release_year YEAR DEFAULT NULL,
                      language_id TINYINT UNSIGNED NOT NULL,
                      original_language_id TINYINT UNSIGNED DEFAULT NULL,
                      rental_duration TINYINT UNSIGNED NOT NULL DEFAULT 3,
                      rental_rate DECIMAL(4,2) NOT NULL DEFAULT 4.99,
                      length SMALLINT UNSIGNED DEFAULT NULL,
                      replacement_cost DECIMAL(5,2) NOT NULL DEFAULT 19.99,
                      rating ENUM('G','PG','PG-13','R','NC-17') DEFAULT 'G',
                      special_features SET('Trailers','Commentaries','Deleted Scenes','Behind the Scenes') DEFAULT NULL,
                      last_update TIMESTAMP NOT NULL ,
                      PRIMARY KEY  (film_id),
                      KEY idx_title (title),
                      KEY idx_fk_language_id (language_id),
                      KEY idx_fk_original_language_id (original_language_id),
                      CONSTRAINT fk_film_language FOREIGN KEY (language_id) REFERENCES language (language_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                      CONSTRAINT fk_film_language_original FOREIGN KEY (original_language_id) REFERENCES language (language_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE film_actor (
                            actor_id SMALLINT UNSIGNED NOT NULL,
                            film_id SMALLINT UNSIGNED NOT NULL,
                            last_update TIMESTAMP NOT NULL ,
                            PRIMARY KEY  (actor_id,film_id),
                            KEY idx_fk_film_id (`film_id`),
                            CONSTRAINT fk_film_actor_actor FOREIGN KEY (actor_id) REFERENCES actor (actor_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                            CONSTRAINT fk_film_actor_film FOREIGN KEY (film_id) REFERENCES film (film_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE film_category (
                               film_id SMALLINT UNSIGNED NOT NULL,
                               category_id TINYINT UNSIGNED NOT NULL,
                               last_update TIMESTAMP NOT NULL ,
                               PRIMARY KEY (film_id, category_id),
                               CONSTRAINT fk_film_category_film FOREIGN KEY (film_id) REFERENCES film (film_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                               CONSTRAINT fk_film_category_category FOREIGN KEY (category_id) REFERENCES category (category_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;










SET @old_default_storage_engine = @@default_storage_engine;
SET @@default_storage_engine = 'MyISAM';

CREATE TABLE film_text (
                           film_id SMALLINT NOT NULL,
                           title VARCHAR(255) NOT NULL,
                           description TEXT,
                           PRIMARY KEY  (film_id),
                           FULLTEXT KEY idx_title_description (title,description)
) DEFAULT CHARSET=utf8mb4;

SET @@default_storage_engine = @old_default_storage_engine;






CREATE TABLE inventory (
                           inventory_id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                           film_id SMALLINT UNSIGNED NOT NULL,
                           store_id TINYINT UNSIGNED NOT NULL,
                           last_update TIMESTAMP NOT NULL ,
                           PRIMARY KEY  (inventory_id),
                           KEY idx_fk_film_id (film_id),
                           KEY idx_store_id_film_id (store_id,film_id),
                           CONSTRAINT fk_inventory_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                           CONSTRAINT fk_inventory_film FOREIGN KEY (film_id) REFERENCES film (film_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE language (
                          language_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                          name CHAR(20) NOT NULL,
                          last_update TIMESTAMP NOT NULL ,
                          PRIMARY KEY (language_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE payment (
                         payment_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
                         customer_id SMALLINT UNSIGNED NOT NULL,
                         staff_id TINYINT UNSIGNED NOT NULL,
                         rental_id INT DEFAULT NULL,
                         amount DECIMAL(5,2) NOT NULL,
                         payment_date DATETIME NOT NULL,
                         last_update TIMESTAMP ,
                         PRIMARY KEY  (payment_id),
                         KEY idx_fk_staff_id (staff_id),
                         KEY idx_fk_customer_id (customer_id),
                         CONSTRAINT fk_payment_rental FOREIGN KEY (rental_id) REFERENCES rental (rental_id) ON DELETE SET NULL ON UPDATE CASCADE,
                         CONSTRAINT fk_payment_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                         CONSTRAINT fk_payment_staff FOREIGN KEY (staff_id) REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;






CREATE TABLE rental (
                        rental_id INT NOT NULL AUTO_INCREMENT,
                        rental_date DATETIME NOT NULL,
                        inventory_id MEDIUMINT UNSIGNED NOT NULL,
                        customer_id SMALLINT UNSIGNED NOT NULL,
                        return_date DATETIME DEFAULT NULL,
                        staff_id TINYINT UNSIGNED NOT NULL,
                        last_update TIMESTAMP NOT NULL ,
                        PRIMARY KEY (rental_id),
                        UNIQUE KEY  (rental_date,inventory_id,customer_id),
                        KEY idx_fk_inventory_id (inventory_id),
                        KEY idx_fk_customer_id (customer_id),
                        KEY idx_fk_staff_id (staff_id),
                        CONSTRAINT fk_rental_staff FOREIGN KEY (staff_id) REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                        CONSTRAINT fk_rental_inventory FOREIGN KEY (inventory_id) REFERENCES inventory (inventory_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                        CONSTRAINT fk_rental_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE staff (
                       staff_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                       first_name VARCHAR(45) NOT NULL,
                       last_name VARCHAR(45) NOT NULL,
                       address_id SMALLINT UNSIGNED NOT NULL,
                       picture BLOB DEFAULT NULL,
                       email VARCHAR(50) DEFAULT NULL,
                       store_id TINYINT UNSIGNED NOT NULL,
                       active BOOLEAN NOT NULL DEFAULT TRUE,
                       username VARCHAR(16) NOT NULL,
                       password VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
                       last_update TIMESTAMP NOT NULL ,
                       PRIMARY KEY  (staff_id),
                       KEY idx_fk_store_id (store_id),
                       KEY idx_fk_address_id (address_id),
                       CONSTRAINT fk_staff_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                       CONSTRAINT fk_staff_address FOREIGN KEY (address_id) REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE TABLE store (
                       store_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                       manager_staff_id TINYINT UNSIGNED NOT NULL,
                       address_id SMALLINT UNSIGNED NOT NULL,
                       last_update TIMESTAMP NOT NULL ,
                       PRIMARY KEY  (store_id),
                       UNIQUE KEY idx_unique_manager (manager_staff_id),
                       KEY idx_fk_address_id (address_id),
                       CONSTRAINT fk_store_staff FOREIGN KEY (manager_staff_id) REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE,
                       CONSTRAINT fk_store_address FOREIGN KEY (address_id) REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;





CREATE VIEW customer_list
AS
SELECT cu.customer_id AS ID, CONCAT(cu.first_name, _utf8mb4' ', cu.last_name) AS name, a.address AS address, a.postal_code AS `zip code`,
       a.phone AS phone, city.city AS city, country.country AS country, IF(cu.active, _utf8mb4'active',_utf8mb4'') AS notes, cu.store_id AS SID
FROM customer AS cu JOIN address AS a ON cu.address_id = a.address_id JOIN city ON a.city_id = city.city_id
                    JOIN country ON city.country_id = country.country_id;





CREATE VIEW film_list
AS
SELECT film.film_id AS FID, film.title AS title, film.description AS description, category.name AS category, film.rental_rate AS price,
       film.length AS length, film.rating AS rating, GROUP_CONCAT(CONCAT(actor.first_name, _utf8mb4' ', actor.last_name) SEPARATOR ', ') AS actors
FROM film LEFT JOIN film_category ON film_category.film_id = film.film_id
          LEFT JOIN category ON category.category_id = film_category.category_id LEFT
              JOIN film_actor ON film.film_id = film_actor.film_id LEFT JOIN actor ON
        film_actor.actor_id = actor.actor_id
GROUP BY film.film_id, category.name;





CREATE VIEW nicer_but_slower_film_list
AS
SELECT film.film_id AS FID, film.title AS title, film.description AS description, category.name AS category, film.rental_rate AS price,
       film.length AS length, film.rating AS rating, GROUP_CONCAT(CONCAT(CONCAT(UCASE(SUBSTR(actor.first_name,1,1)),
                                                                                LCASE(SUBSTR(actor.first_name,2,LENGTH(actor.first_name))),_utf8mb4' ',CONCAT(UCASE(SUBSTR(actor.last_name,1,1)),
                                                                                                                                                              LCASE(SUBSTR(actor.last_name,2,LENGTH(actor.last_name)))))) SEPARATOR ', ') AS actors
FROM film LEFT JOIN film_category ON film_category.film_id = film.film_id
          LEFT JOIN category ON category.category_id = film_category.category_id LEFT
              JOIN film_actor ON film.film_id = film_actor.film_id LEFT JOIN actor ON
        film_actor.actor_id = actor.actor_id
GROUP BY film.film_id, category.name;





CREATE VIEW staff_list
AS
SELECT s.staff_id AS ID, CONCAT(s.first_name, _utf8mb4' ', s.last_name) AS name, a.address AS address, a.postal_code AS `zip code`, a.phone AS phone,
       city.city AS city, country.country AS country, s.store_id AS SID
FROM staff AS s JOIN address AS a ON s.address_id = a.address_id JOIN city ON a.city_id = city.city_id
                JOIN country ON city.country_id = country.country_id;





CREATE VIEW sales_by_store
AS
SELECT
    CONCAT(c.city, _utf8mb4',', cy.country) AS store
     , CONCAT(m.first_name, _utf8mb4' ', m.last_name) AS manager
     , SUM(p.amount) AS total_sales
FROM payment AS p
         INNER JOIN rental AS r ON p.rental_id = r.rental_id
         INNER JOIN inventory AS i ON r.inventory_id = i.inventory_id
         INNER JOIN store AS s ON i.store_id = s.store_id
         INNER JOIN address AS a ON s.address_id = a.address_id
         INNER JOIN city AS c ON a.city_id = c.city_id
         INNER JOIN country AS cy ON c.country_id = cy.country_id
         INNER JOIN staff AS m ON s.manager_staff_id = m.staff_id
GROUP BY s.store_id
ORDER BY cy.country, c.city;








CREATE VIEW sales_by_film_category
AS
SELECT
    c.name AS category
     , SUM(p.amount) AS total_sales
FROM payment AS p
         INNER JOIN rental AS r ON p.rental_id = r.rental_id
         INNER JOIN inventory AS i ON r.inventory_id = i.inventory_id
         INNER JOIN film AS f ON i.film_id = f.film_id
         INNER JOIN film_category AS fc ON f.film_id = fc.film_id
         INNER JOIN category AS c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY total_sales DESC;

SET FOREIGN_KEY_CHECKS=1;


