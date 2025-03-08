	SELECT        I.IssueID
		, charindex('guid=',i.body, CHARINDEX('https://', I.Body))
		,[GuidDebug] = case 
			when charindex('guid=',i.body,CHARINDEX('https://', I.Body)) > 0
				then SUBSTRING(i.Body, charindex('guid=',i.body,CHARINDEX('https://', I.Body)), 50)
			when charindex('guid%3D',i.body, CHARINDEX('https://', I.Body)) > 0 
				then SUBSTRING(i.Body, charindex('guid%3D',i.body,CHARINDEX('https://', I.Body)), 50)
				end

		,[Guid] = case 
			when charindex('guid=',i.body,CHARINDEX('https://', I.Body)) > 0
				then SUBSTRING(i.Body, charindex('guid=',i.body,CHARINDEX('https://', I.Body))+5, 36)
			when charindex('guid%3D',i.body, CHARINDEX('https://', I.Body)) > 0 
				then SUBSTRING(i.Body, charindex('guid%3D',i.body,CHARINDEX('https://', I.Body))+7, 36)
				end

		, charindex('guid%3D',i.body, CHARINDEX('https://', I.Body))
				,[Guid3d] = SUBSTRING(i.Body, charindex('guid%3D',i.body,CHARINDEX('https://', I.Body))+5, 36)
		, CHARINDEX('https://', I.Body) 

				,CHARINDEX('"', I.Body, CHARINDEX('https://', I.Body) )
				,SUBSTRING(i.Body, CHARINDEX('https://', I.Body), 100)
				-- ,SUBSTRING(I.Body, CHARINDEX('https://', I.Body), CHARINDEX('"', I.Body, CHARINDEX('https://', I.Body)) - CHARINDEX('https://', I.Body))
				--,CASE
				--	WHEN CHARINDEX('https://', I.Body) > 0 THEN
				--		SUBSTRING(I.Body, CHARINDEX('https://', I.Body), CHARINDEX('"', I.Body, CHARINDEX('https://', I.Body)) - CHARINDEX('https://', I.Body))
				--	ELSE 'NULL'
				--END AS IDREURL
				, I.IssueDate
				, I.[Subject] 
				, I.Body 
				
	FROM		[JitBitArbitration].[dbo].[hdIssues] I
	where i.categoryID = 486
	and CHARINDEX('guid',i.body) > 0

--https://nam12.safelinks.protection.outlook.com/?url=https%3A%2F%2Fwww.cms.gov%2FCCIIO%2FResources%2F
--https://nam12.safelinks.protection.outlook.com/?url=https%3A%2F%2Fwww.cms.gov%2FCCIIO%2FResources%2F
--https://nam12.safelinks.protection.outlook.com/?url=https%3A%2F%2Furldefense.com%2Fv3%2F__https%3A%2