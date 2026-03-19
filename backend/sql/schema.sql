SET NAMES utf8mb4;
SET time_zone = '+00:00';

CREATE TABLE IF NOT EXISTS ingest_runs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    status ENUM('running', 'success', 'failed') NOT NULL DEFAULT 'running',
    source_root VARCHAR(1024) NOT NULL,
    message TEXT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS source_files (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ingest_run_id BIGINT UNSIGNED NOT NULL,
    relative_path VARCHAR(1024) NOT NULL,
    file_type ENUM('report', 'detail', 'other') NOT NULL,
    file_hash CHAR(64) NULL,
    parse_status ENUM('parsed', 'skipped', 'failed') NOT NULL,
    parse_message TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_source_files_ingest_run (ingest_run_id),
    KEY idx_source_files_type_status (file_type, parse_status),
    CONSTRAINT fk_source_files_ingest_runs
        FOREIGN KEY (ingest_run_id) REFERENCES ingest_runs(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS universities (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    external_university_id INT UNSIGNED NOT NULL,
    name VARCHAR(512) NOT NULL,
    rector_name VARCHAR(255) NULL,
    address VARCHAR(512) NULL,
    website_url VARCHAR(512) NULL,
    report_file VARCHAR(255) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_universities_external_id (external_university_id),
    FULLTEXT KEY ft_universities_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS programs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    university_id BIGINT UNSIGNED NOT NULL,
    faculty_name VARCHAR(512) NOT NULL,
    program_code VARCHAR(32) NULL,
    program_name VARCHAR(512) NOT NULL,
    specialization_name VARCHAR(512) NULL,
    study_form VARCHAR(255) NULL,
    payment_type VARCHAR(64) NULL,
    annual_fee_som INT NULL,
    admission_plan INT NULL,
    threshold_text VARCHAR(1024) NULL,
    threshold_main_score INT NULL,
    registered_count_reported INT NULL,
    detail_report_file VARCHAR(255) NULL,
    contact_file VARCHAR(255) NULL,
    source_report_file VARCHAR(255) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_program_business (
        university_id,
        faculty_name(191),
        program_code,
        program_name(191),
        payment_type
    ),
    UNIQUE KEY uq_program_detail_file (detail_report_file),
    KEY idx_programs_university (university_id),
    KEY idx_programs_code (program_code),
    FULLTEXT KEY ft_programs_name (program_name, specialization_name),
    CONSTRAINT fk_programs_universities
        FOREIGN KEY (university_id) REFERENCES universities(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS program_thresholds (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    program_id BIGINT UNSIGNED NOT NULL,
    subject_name VARCHAR(128) NOT NULL,
    min_score INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_program_threshold_subject (program_id, subject_name),
    CONSTRAINT fk_program_thresholds_programs
        FOREIGN KEY (program_id) REFERENCES programs(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS program_rounds (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    program_id BIGINT UNSIGNED NOT NULL,
    round_number INT NOT NULL,
    registered_count INT NULL,
    admitted_confirmed INT NULL,
    recommended_count INT NULL,
    vacancies_total INT NULL,
    summary_text VARCHAR(1024) NULL,
    source_detail_file VARCHAR(255) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_program_round (program_id, round_number),
    KEY idx_program_rounds_source (source_detail_file),
    CONSTRAINT fk_program_rounds_programs
        FOREIGN KEY (program_id) REFERENCES programs(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS competition_categories (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    program_round_id BIGINT UNSIGNED NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    cutoff_value DECIMAL(10, 2) NULL,
    rows_count INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_round_category (program_round_id, category_name),
    KEY idx_categories_round (program_round_id),
    CONSTRAINT fk_categories_program_rounds
        FOREIGN KEY (program_round_id) REFERENCES program_rounds(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ranking_rows_anonymized (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    competition_category_id BIGINT UNSIGNED NOT NULL,
    rank_position INT NOT NULL,
    primary_score INT NULL,
    additional_score INT NULL,
    total_score INT NULL,
    registration_datetime DATETIME NULL,
    is_recommended TINYINT(1) NOT NULL DEFAULT 0,
    recommendation_note VARCHAR(255) NULL,
    row_fingerprint CHAR(64) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ranking_row_fingerprint (row_fingerprint),
    KEY idx_ranking_category_rank (competition_category_id, rank_position),
    KEY idx_ranking_category_score (competition_category_id, total_score),
    CONSTRAINT fk_ranking_categories
        FOREIGN KEY (competition_category_id) REFERENCES competition_categories(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS chance_snapshots (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    program_round_id BIGINT UNSIGNED NOT NULL,
    category_name VARCHAR(255) NULL,
    input_total_score INT NOT NULL,
    estimated_rank INT NOT NULL,
    admission_plan INT NOT NULL,
    threshold_main_score INT NULL,
    current_cutoff_score INT NULL,
    chance_level ENUM('high', 'medium', 'low') NOT NULL,
    explanation TEXT NOT NULL,
    calculated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_chance_round_score (program_round_id, input_total_score),
    CONSTRAINT fk_chance_program_rounds
        FOREIGN KEY (program_round_id) REFERENCES program_rounds(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
