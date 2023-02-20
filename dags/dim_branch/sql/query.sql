/*Иерархия территорий Последний 13.02.2023*/
    SELECT 
        bdb.ID as id_branch         
        ,bdb.Name as branch_name     
        ,odth.id2 fk_division
        ,odth.Name2 as division_name
        ,odth.Id3 as fk_rrs_lvl_1  
        ,odth.Name3 as rrs_lvl_1_name
        ,odth.Id4 as fk_rrs_lvl_2
        ,odth.Name4 as rrs_lvl_2_name
        ,convert(datetime, SUBSTRING(CAST(DateOpenID AS nvarchar), 0, 9) ) as date_open
        ,convert(datetime, SUBSTRING(CAST(DateCloseID AS nvarchar), 0, 9) ) as date_close
        ,bdb.[Type] as branch_type
        ,bdc.id as fk_city
        ,bdc.Name as city_name
        ,bdr.ID as fk_region
        ,bdr.Name as region_name
        ,[is_open] = 
                    CASE 
                        WHEN convert(datetime, SUBSTRING(CAST(DateCloseID AS nvarchar), 0, 9) ) IS NULL THEN 'Действующий'
                        ELSE 'Закрытый'
                    END
        , TotalArea as total_area
        , SalesArea as sales_area
        , RentalRate as rental_rate
        , bdb.Code as code_branch
        , LOWER(access.dbo_ConvertBinaryToGuid(bdb.SourceID)) as source_id
        , LOWER(access.dbo_ConvertBinaryToGuid(bdc.SourceID)) as city_source_id
        , odth.Code2 as code_division
        , odth.Code3 rrs_lvl_1_code
        , odth.Code4 rrs_lvl_2_code 
        , bdb.Format as format
        FROM olap_DIM_Territory_Hierarchy odth 
            CROSS JOIN branch_DIM_Branch bdb
            CROSS JOIN branch_DIM_City bdc
            CROSS JOIN branch_DIM_Region bdr
        WHERE 
            Id4 = bdb.TerritoryID
            AND bdc.RegionID = bdr.ID
            AND bdc.ID = bdb.CityID
            and Id2 in (876, 877, 871, 874, 875, 878, 872, 873, 993, 798, 790)
