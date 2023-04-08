-- Create tables

CREATE TABLE `episodes` ( 
  `series_id` String NOT NULL, 
  `season_id` String NOT NULL, 
  `episode_id` String NOT NULL, 
  `title` Utf8, 
  `air_date` Timestamp, 
  `views` Uint64, 
  INDEX `index_episodes_air_date` GLOBAL ON ( `air_date` ), 
  PRIMARY KEY ( `series_id`, `season_id`, `episode_id` ) 
);

CREATE TABLE `seasons` ( 
  `series_id` String NOT NULL, 
  `season_id` String NOT NULL, 
  `title` Utf8, 
  `first_aired` Timestamp, 
  `last_aired` Timestamp, 
  INDEX `index_season_first_aired` GLOBAL ON ( `first_aired` ), 
  INDEX `index_series_title` GLOBAL ON ( `title` ), 
  PRIMARY KEY ( `series_id`, `season_id` ) 
);

CREATE TABLE `series` ( 
  `series_id` String NOT NULL, 
  `title` Utf8, 
  `series_info` Utf8, 
  `release_date` Timestamp, 
  `comment` Utf8, 
  INDEX `index_series_title` GLOBAL ON ( `title` ), 
  PRIMARY KEY ( `series_id` ) 
);