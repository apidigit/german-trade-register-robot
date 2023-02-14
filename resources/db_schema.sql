
				CREATE TABLE IF NOT EXISTS companies(
                    `id` VARCHAR(255) NOT NULL PRIMARY KEY,
                    `register_references` VARCHAR(255)  NOT NULL,
                    `name` VARCHAR(255) NOT NULL,
                    `headquarter_city` VARCHAR(255) NOT NULL,
                    `headquarter_postal_code` VARCHAR(15),
                    `headquarter_street` VARCHAR(255),
                    `headquarter_address_supplement` VARCHAR(255),
                    `currently_registered` BOOLEAN,
                    `business_purpose` VARCHAR(5000),
                    `share_capital_amount` VARCHAR(15),
                    `share_capital_currency` VARCHAR(15),
                    `incorporation_date` VARCHAR(255),
                    `last_registry_update` VARCHAR(255),
                    `phone` VARCHAR(255),
                    `mobile` VARCHAR(255),
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35));

				CREATE TABLE IF NOT EXISTS company_ceos(
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    				`company_id` VARCHAR(255) NOT NULL,
                    `titel` VARCHAR(15) NULL,
                    `gender` VARCHAR(15) NULL,
                    `first_name` VARCHAR(55),
                    `last_name` VARCHAR(55) NOT NULL,
    				`birth_name` VARCHAR(255),
                    `residence_city` VARCHAR(255),
                    `birthdate` VARCHAR(15),
    				`create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                	CONSTRAINT `uc_ceo` UNIQUE (`company_id`, `first_name`, `last_name`));

 				CREATE TABLE IF NOT EXISTS company_histories(
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `position` int NOT NULL,
                    `name` VARCHAR(255) NOT NULL,
                    `city` VARCHAR(255) NOT NULL,
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                	CONSTRAINT `uc_history` UNIQUE (`company_id`, `name`, `city`));

                 CREATE TABLE IF NOT EXISTS company_procura(
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `titel` VARCHAR(15) NULL,
                    `gender` VARCHAR(15) NULL,
                    `first_name` VARCHAR(55),
                    `last_name` VARCHAR(55) NOT NULL,
                    `email` VARCHAR(255),
                    `mobile` VARCHAR(255),
                    `phone` VARCHAR(255),
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                	CONSTRAINT `uc_procura` UNIQUE (`company_id`, `first_name`, `last_name`));

                CREATE TABLE IF NOT EXISTS company_locations(
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `city` VARCHAR(255) NOT NULL,
                    `postal_code` VARCHAR(15) NOT NULL,
                    `street` VARCHAR(255) NOT NULL,
                    `address_supplement` VARCHAR(255),
                    `email` VARCHAR(255),
                    `phone` VARCHAR(255),
                    `mobile` VARCHAR(255),
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                	CONSTRAINT `uc_location` UNIQUE (`company_id`, `city`, `postal_code`, `street`));

                  CREATE TABLE IF NOT EXISTS company_contacts(
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `titel` VARCHAR(15) NULL,
                    `gender` VARCHAR(15) NULL,
                    `first_name` VARCHAR(55),
                    `last_name` VARCHAR(55) NOT NULL,
                    `position` VARCHAR(255),
                    `location` BIGINT NULL,
                    `office_email` VARCHAR(255),
                    `private_email` VARCHAR(255),
                    `office_mobile` VARCHAR(255),
                    `office_phone` VARCHAR(255),
                    `private_mobile` VARCHAR(255),
                    `private_phone` VARCHAR(255),
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
					FOREIGN KEY (`location`) REFERENCES company_locations(`id`),
                	CONSTRAINT `uc_contact` UNIQUE (`company_id`, `first_name`, `last_name`, `office_email`));

