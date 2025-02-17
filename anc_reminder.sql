WITH ANC_Obs_with_Component AS (
  SELECT
    `Observation's Meta Source` AS Source,
    `Patient's Logical ID` AS Id,
    `Patient's Identifier Value <System = 'Project ID: Mother'>` AS Identifier,
    JSON_VALUE(patient_name, '$.text') AS Name,
    `Patient's Address 0 District` AS District,
    `Patient's Address 0 City` AS City,
    JSON_VALUE(patient_telecom, '$.value') AS Telecom,
    `Encounter's ReasonCode Text <Coding 0 Display = 'Visit date'>` AS last_visit_date,
    obs_component
  FROM 
    `nextgen-398301.fhir_dataset_multiple_sources.Flattened Health Record - Observation-based`,
    UNNEST(`Observation's Component`) AS obs_component,
    UNNEST(`Patient's Name`) AS patient_name,
    UNNEST(`Patient's Telecom`) AS patient_telecom
  WHERE 
    JSON_VALUE(obs_component, '$.code.coding[0].system') = 'http://loinc.org'
    AND PARSE_DATE('%Y-%m-%d', `Encounter's ReasonCode Text <Coding 0 Display = 'Visit date'>`) > DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL {self.last_visit_week_range} WEEK) 
    AND ( `Patient's Active` = 'true' OR `Patient's Active` is NULL)
),

ANC_Obs_wo_Component AS (
  SELECT
    `Observation's Meta Source` AS Source,
    `Patient's Logical ID` AS Id,
    `Patient's Identifier Value <System = 'Project ID: Mother'>` AS Identifier,
    JSON_VALUE(patient_name, '$.text') AS Name,
    `Patient's Address 0 District` AS District,
    `Patient's Address 0 City` AS City,
    JSON_VALUE(patient_telecom, '$.value') AS Telecom,
    `Observation's ValueX`,
    `Encounter's ReasonCode Text <Coding 0 Display = 'Visit date'>` AS last_visit_date,
    `Observation's Code Coding 0 Code`
  FROM 
    `nextgen-398301.fhir_dataset_multiple_sources.Flattened Health Record - Observation-based`,
    UNNEST(`Patient's Name`) AS patient_name,
    UNNEST(`Patient's Telecom`) AS patient_telecom
  WHERE 
    `Observation's Code Coding 0 System` = 'http://loinc.org'
    AND PARSE_DATE('%Y-%m-%d', `Encounter's ReasonCode Text <Coding 0 Display = 'Visit date'>`) > DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL {self.last_visit_week_range} WEEK) 
    AND `Patient's Active` = 'true'
)

SELECT Source, Id, A.Identifier, Name, A.Telecom, District, City, last_mens_date, gestational_age, Trimester, last_visit_date, next_visit_date
FROM (
  SELECT 
    Source, Id, Identifier, Name, Telecom, District, City, last_mens_date, gestational_age, Trimester, last_visit_date, 
    DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL {self.days_before_wa} DAY) AS next_visit_date
  FROM (
    SELECT 
      Source, Id, Identifier, Name, District, City, Telecom, last_visit_date, last_mens_date, gestational_age, Trimester,
      CASE Trimester
        WHEN 1 THEN 4
        WHEN 2 THEN 2
        WHEN 3 THEN 1
      END AS week_range,
      CASE last_mens_date
        WHEN '' THEN -1
        ELSE (DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), PARSE_DATE('%Y-%m-%d', last_mens_date), DAY) + {self.days_before_wa} )
      END AS day_diff,
      ROW_NUMBER() OVER (PARTITION BY Identifier ORDER BY last_mens_date DESC, gestational_age DESC, Trimester DESC) AS RowNum_last_mens_date
    FROM (
      SELECT
        Source, Id, Identifier, Name, District, City, Telecom, last_visit_date,
        MAX(last_mens_date) AS last_mens_date,
        MAX(gestational_age) AS gestational_age,
        MAX(Trimester) AS Trimester
      FROM (
        SELECT
          Source, Id, Identifier, Name, District, City, Telecom, last_mens_date, last_visit_date, gestational_age,
          CASE 
            WHEN gestational_age = 0 THEN 0
            WHEN gestational_age < 28 THEN 1
            WHEN gestational_age < 36 THEN 2
            WHEN gestational_age < 41 THEN 3
          ELSE -1
          END AS Trimester,
          ROW_NUMBER() OVER (PARTITION BY Identifier ORDER BY gestational_age DESC) AS RowNum_gestational_age
        FROM (
          SELECT 
            Source, Id, Identifier, Name, District, City, Telecom, last_mens_date, last_visit_date,
            DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), PARSE_DATE('%Y-%m-%d', last_mens_date), WEEK) AS gestational_age
          FROM (
            SELECT
              Source, Id, Identifier, Name, District, City, Telecom, last_visit_date,
              JSON_VALUE(obs_component, '$.valueDateTime') AS last_mens_date
            FROM ANC_Obs_with_Component
            WHERE 
              JSON_VALUE(obs_component, '$.code.coding[0].code') = '8665-2'
              AND PARSE_DATE('%Y-%m-%d', JSON_VALUE(obs_component, '$.valueDateTime')) > DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 41 WEEK)
          )
          UNION ALL
          SELECT 
            Source, Id, Identifier, Name, District, City, Telecom, '' AS last_mens_date, last_visit_date,
            CAST(week_age_mens AS INT64) + DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), PARSE_DATE('%Y-%m-%d', last_visit_date), WEEK) AS gestational_age
          FROM (
            SELECT
              Source, Id, Identifier, Name, District, City, Telecom, last_visit_date,
              JSON_VALUE(obs_component, '$.valueQuantity.value') AS week_age_mens
            FROM ANC_Obs_with_Component
            WHERE JSON_VALUE(obs_component, '$.code.coding[0].code') IN ('11885-1', '11888-5')
          )
          UNION ALL
          SELECT 
            Source, Id, Identifier, Name, District, City, Telecom, last_mens_date, last_visit_date,
            DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), PARSE_DATE('%Y-%m-%d', last_mens_date), WEEK) AS gestational_age
          FROM (
            SELECT
              Source, Id, Identifier, Name, District, City, Telecom, last_visit_date,
              JSON_VALUE(`Observation's ValueX`, '$') AS last_mens_date
            FROM ANC_Obs_wo_Component
            WHERE 
              `Observation's Code Coding 0 Code` = '8665-2'
              AND PARSE_DATE('%Y-%m-%d', JSON_VALUE(`Observation's ValueX`, '$')) > DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 41 WEEK)
          )
          UNION ALL
          SELECT 
            Source, Id, Identifier, Name, District, City, Telecom, '' AS last_mens_date, last_visit_date,
            CAST(CAST(week_age_mens AS DECIMAL) AS INT64) + DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), PARSE_DATE('%Y-%m-%d', last_visit_date), WEEK) AS gestational_age
          FROM (
            SELECT
              Source, Id, Identifier, Name, District, City, Telecom, last_visit_date, 
              JSON_VALUE(`Observation's ValueX`, '$.value') AS week_age_mens
            FROM ANC_Obs_wo_Component
            WHERE `Observation's Code Coding 0 Code` IN ('11885-1', '11888-5')
          )
        )
      )
      WHERE 
        RowNum_gestational_age = 1
        AND Trimester > 0
      GROUP BY Source, Id, Identifier, Name, District, City, Telecom, last_visit_date
    )
  )
  WHERE
    RowNum_last_mens_date = 1
    AND (
      (
        day_diff > 0
        AND MOD(day_diff, 7) = 0
        AND MOD(DIV(day_diff, 7), week_range) = 0
      ) OR (
        day_diff < 0
        AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('Asia/Jakarta')) = (Trimester + {self.days_before_wa} - 1) 
      )
    )
) AS A
LEFT JOIN (
  SELECT
    `Patient's Identifier Value <System = 'Project ID: Mother'>` AS Identifier,
    JSON_VALUE(patient_telecom, '$.value') AS Telecom
  FROM 
    `nextgen-398301.fhir_dataset_multiple_sources.Flattened Health Record - Condition-based`,
    UNNEST(`Condition's Code Coding`) AS Condition,
    UNNEST(`Patient's Telecom`) AS patient_telecom
  WHERE 
    JSON_VALUE(Condition, '$.code') IN ('234234234', '86569001')
    AND EXTRACT(DATE FROM `Condition's Meta LastUpdated`) > DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 41 WEEK)
) AS B
ON 
  A.Identifier = B.Identifier
  OR A.Telecom = B.Telecom
LEFT JOIN (
  SELECT identifier, mobile_no
  FROM `nextgen-398301.fhir_wa.sent_status`
  WHERE 
    template = '{self.message_template_id}'
    AND EXTRACT(DATE FROM `timestamp`) = CURRENT_DATE('Asia/Jakarta')
) AS C
ON 
  A.Identifier = C.identifier
  OR A.telecom = C.mobile_no
WHERE 
  B.identifier is NULL
  AND C.identifier is NULL