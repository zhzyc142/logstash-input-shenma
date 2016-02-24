module LogStash::PluginMixins::ShenmaSql
  public 

  def mulit_buy_sql(buyer_userid, time_begin, time_end)
    "SELECT
      `GroupBy1`.`K1` AS `CustomerId`,
      `GroupBy1`.`A1` AS `C1`
    FROM
      (
        SELECT
          `Extent1`.`CustomerId` AS `K1`,
          COUNT(1) AS `A1`
        FROM
          `Order` AS `Extent1`
        INNER JOIN `IMS_AssociateIncomeHistory` AS `Extent2` ON `Extent1`.`OrderNo` = `Extent2`.`SourceNo`
        WHERE
          (
            (
              (`Extent1`.`Status` >= 0)
              AND (
                `Extent1`.`CreateDate` < '#{time_begin}'
              )
            )
            AND (
              `Extent1`.`CustomerId` IN (#{buyer_userid}) 
            )
          )
        AND (
          `Extent2`.`AssociateUserId` = '#{time_end}'
        )
        GROUP BY
          `Extent1`.`CustomerId`
      ) AS `GroupBy1`
    "
  end

  def  new_add_orders_sql(time_begin, time_end)
    "SELECT
      `Project2`.`Id`,
      `Project2`.`C1`,
      `Project2`.`Id1`,
      `Project2`.`OrderNo`,
      `Project2`.`CustomerId`,
      `Project2`.`CoinAmount`,
      `Project2`.`DiscountAmount`,
      `Project2`.`TotalAmount`,
      `Project2`.`RecAmount`,
      `Project2`.`Status`,
      `Project2`.`PaymentMethodCode`,
      `Project2`.`PaymentMethodName`,
      `Project2`.`PaymentMethodType`,
      `Project2`.`ShippingZipCode`,
      `Project2`.`ShippingAddress`,
      `Project2`.`ShippingContactPerson`,
      `Project2`.`ShippingContactPhone`,
      `Project2`.`NeedInvoice`,
      `Project2`.`InvoiceSubject`,
      `Project2`.`InvoiceDetail`,
      `Project2`.`IsInvoiced`,
      `Project2`.`InvoicedDate`,
      `Project2`.`ShippingFee`,
      `Project2`.`CreateDate`,
      `Project2`.`CreateUser`,
      `Project2`.`UpdateDate`,
      `Project2`.`UpdateUser`,
      `Project2`.`Memo`,
      `Project2`.`InvoiceAmount`,
      `Project2`.`OrderSource`,
      `Project2`.`OrderProductType`
    FROM
      (
        SELECT
          `Distinct1`.`Id`,
          `Filter2`.`Id` AS `Id1`,
          `Filter2`.`OrderNo`,
          `Filter2`.`CustomerId`,
          `Filter2`.`CoinAmount`,
          `Filter2`.`DiscountAmount`,
          `Filter2`.`TotalAmount`,
          `Filter2`.`RecAmount`,
          `Filter2`.`Status`,
          `Filter2`.`PaymentMethodCode`,
          `Filter2`.`PaymentMethodName`,
          `Filter2`.`PaymentMethodType`,
          `Filter2`.`ShippingZipCode`,
          `Filter2`.`ShippingAddress`,
          `Filter2`.`ShippingContactPerson`,
          `Filter2`.`ShippingContactPhone`,
          `Filter2`.`NeedInvoice`,
          `Filter2`.`InvoiceSubject`,
          `Filter2`.`InvoiceDetail`,
          `Filter2`.`IsInvoiced`,
          `Filter2`.`InvoicedDate`,
          `Filter2`.`ShippingFee`,
          `Filter2`.`CreateDate`,
          `Filter2`.`CreateUser`,
          `Filter2`.`UpdateDate`,
          `Filter2`.`UpdateUser`,
          `Filter2`.`Memo`,
          `Filter2`.`InvoiceAmount`,
          `Filter2`.`OrderSource`,
          `Filter2`.`OrderProductType`,
          CASE
        WHEN (`Filter2`.`Id` IS NULL) THEN
          (NULL)
        ELSE
          (1)
        END AS `C1`
        FROM
          (
            SELECT DISTINCT
              `Extent3`.`Id`
            FROM
              `Order` AS `Extent1`
            INNER JOIN `IMS_AssociateIncomeHistory` AS `Extent2` ON `Extent1`.`OrderNo` = `Extent2`.`SourceNo`
            INNER JOIN `IMS_Associate` AS `Extent3` ON `Extent2`.`AssociateUserId` = `Extent3`.`UserId`
            WHERE
              (
                (
                  (
                    `Extent1`.`CreateDate` >= '#{time_begin}'
                  )
                  AND (
                    `Extent1`.`CreateDate` < '#{time_end}'
                  )
                )
                AND (`Extent1`.`Status` >= 1)
              )
            AND (1 = `Extent3`.`Status`)
          ) AS `Distinct1`
        LEFT OUTER JOIN (
          SELECT
            `Extent4`.`Id`,
            `Extent4`.`OrderNo`,
            `Extent4`.`CustomerId`,
            `Extent4`.`CoinAmount`,
            `Extent4`.`DiscountAmount`,
            `Extent4`.`TotalAmount`,
            `Extent4`.`RecAmount`,
            `Extent4`.`Status`,
            `Extent4`.`PaymentMethodCode`,
            `Extent4`.`PaymentMethodName`,
            `Extent4`.`PaymentMethodType`,
            `Extent4`.`ShippingZipCode`,
            `Extent4`.`ShippingAddress`,
            `Extent4`.`ShippingContactPerson`,
            `Extent4`.`ShippingContactPhone`,
            `Extent4`.`NeedInvoice`,
            `Extent4`.`InvoiceSubject`,
            `Extent4`.`InvoiceDetail`,
            `Extent4`.`IsInvoiced`,
            `Extent4`.`InvoicedDate`,
            `Extent4`.`ShippingFee`,
            `Extent4`.`CreateDate`,
            `Extent4`.`CreateUser`,
            `Extent4`.`UpdateDate`,
            `Extent4`.`UpdateUser`,
            `Extent4`.`Memo`,
            `Extent4`.`InvoiceAmount`,
            `Extent4`.`OrderSource`,
            `Extent4`.`OrderProductType`,
            `Extent5`.`Id` AS `ID1`,
            `Extent5`.`SourceType`,
            `Extent5`.`SourceNo`,
            `Extent5`.`AssociateUserId`,
            `Extent5`.`AssociateIncome`,
            `Extent5`.`Status` AS `STATUS1`,
            `Extent5`.`CreateDate` AS `CREATEDATE1`,
            `Extent5`.`UpdateDate` AS `UPDATEDATE1`,
            `Extent5`.`GroupId`,
            `Extent6`.`Id` AS `ID2`,
            `Extent6`.`UserId`,
            `Extent6`.`CreateDate` AS `CREATEDATE2`,
            `Extent6`.`CreateUser` AS `CREATEUSER1`,
            `Extent6`.`Status` AS `STATUS2`,
            `Extent6`.`TemplateId`,
            `Extent6`.`OperateRight`,
            `Extent6`.`StoreId`,
            `Extent6`.`SectionId`,
            `Extent6`.`OperatorCode`,
            `Extent6`.`IsStoreManager`,
            `Extent6`.`ProductOffLineDate`
          FROM
            `Order` AS `Extent4`
          INNER JOIN `IMS_AssociateIncomeHistory` AS `Extent5` ON `Extent4`.`OrderNo` = `Extent5`.`SourceNo`
          INNER JOIN `IMS_Associate` AS `Extent6` ON `Extent5`.`AssociateUserId` = `Extent6`.`UserId`
          WHERE
            `Extent4`.`Status` >= 1
        ) AS `Filter2` ON (
          (
            (
              `Filter2`.`CreateDate` >= '#{time_begin}'
            )
            AND (
              `Filter2`.`CreateDate` < '#{time_end}'
            )
          )
          AND (1 = `Filter2`.`STATUS2`)
        )
        AND (
          `Distinct1`.`Id` = `Filter2`.`ID2`
        )
      ) AS `Project2`
    ORDER BY
      `Project2`.`Id` ASC,
      `Project2`.`C1` ASC"
  end

  def new_add_favorite_sql(time_begin, time_end)
    "SELECT
      `Project2`.`FavoriteSourceId`,
      `Project2`.`C1`,
      `Project2`.`Id`,
      `Project2`.`FavoriteSourceId1`,
      `Project2`.`FavoriteSourceType`,
      `Project2`.`User_Id`,
      `Project2`.`CreatedUser`,
      `Project2`.`CreatedDate`,
      `Project2`.`Description`,
      `Project2`.`Status`,
      `Project2`.`Store_Id`
    FROM
      (
        SELECT
          `Distinct1`.`FavoriteSourceId`,
          `Extent2`.`Id`,
          `Extent2`.`FavoriteSourceId` AS `FavoriteSourceId1`,
          `Extent2`.`FavoriteSourceType`,
          `Extent2`.`User_Id`,
          `Extent2`.`CreatedUser`,
          `Extent2`.`CreatedDate`,
          `Extent2`.`Description`,
          `Extent2`.`Status`,
          `Extent2`.`Store_Id`,
          CASE
        WHEN (`Extent2`.`Id` IS NULL) THEN
          (NULL)
        ELSE
          (1)
        END AS `C1`
        FROM
          (
            SELECT DISTINCT
              `Extent1`.`FavoriteSourceId`
            FROM
              `Favorite` AS `Extent1`
            WHERE
              (
                (
                  (1 = `Extent1`.`Status`)
                  AND (
                    7 = `Extent1`.`FavoriteSourceType`
                  )
                )
                AND (
                  `Extent1`.`CreatedDate` >= '#{time_begin}'
                )
              )
            AND (
              `Extent1`.`CreatedDate` < '#{time_end}'
            )
          ) AS `Distinct1`
        LEFT OUTER JOIN `Favorite` AS `Extent2` ON (
          (
            (
              (1 = `Extent2`.`Status`)
              AND (
                7 = `Extent2`.`FavoriteSourceType`
              )
            )
            AND (
              `Extent2`.`CreatedDate` >=  '#{time_begin}'
            )
          )
          AND (
            `Extent2`.`CreatedDate` < '#{time_end}'
          )
        )
        AND (
          `Distinct1`.`FavoriteSourceId` = `Extent2`.`FavoriteSourceId`
        )
      ) AS `Project2`
    ORDER BY
      `Project2`.`FavoriteSourceId` ASC,
      `Project2`.`C1` ASC" 
  end

  def buyer_everyweek_data_sql(time_begin, time_end)
    "SELECT
      ims_associate.UserId,
      store.Id as storeid,
      user.UserLevel as UserLevel,
      CASE
    WHEN ISNULL(
      ims_associate_apply.`StoreName`
    ) THEN
      store.`Name`
    ELSE
      ims_associate_apply.`StoreName`
    END AS storeName,
     section.Id,
     CASE
    WHEN ISNULL(
      ims_associate_apply.`SectionName`
    ) THEN
      section.`Name`
    ELSE
      ims_associate_apply.`SectionName`
    END AS SectionName,
     section.SectionCode,
     CASE
    WHEN ISNULL(ims_associate_apply.`Name`) THEN
      ims_operator.`Name`
    ELSE
      ims_associate_apply.`Name`
    END AS buyerName,
     (
      SELECT
        IFNULL(sum(totalamount), 0)
      FROM
        `order`
      JOIN ims_associateincomehistory ON `order`.OrderNo = ims_associateincomehistory.SourceNo
      WHERE
        ims_associateincomehistory.AssociateUserId = ims_associate.UserId
      AND `order`.`Status` > 0 and `order`.CreateDate < '#{time_end}' and `order`.CreateDate >= '#{time_begin}'
    ) AS orderAmount,
    (
      SELECT
        IFNULL( sum(RecAmount), 0)
      FROM
        `order`
      JOIN ims_associateincomehistory ON `order`.OrderNo = ims_associateincomehistory.SourceNo
      WHERE
        ims_associateincomehistory.AssociateUserId = ims_associate.UserId
      AND `order`.`Status` > 0  and `order`.CreateDate < '#{time_end}' and `order`.CreateDate >= '#{time_begin}'
    ) AS orderRecivedAmount,
    (
      SELECT
        count(*)
      FROM
        favorite
      WHERE
        FavoriteSourceType = 7
      AND FavoriteSourceId = ims_associate.id
      AND `Status` = 1
    ) AS allfavoriteNumber,
    (
      SELECT
        count(*)
      FROM
        favorite
      WHERE
        FavoriteSourceType = 7
      AND FavoriteSourceId = ims_associate.id
      AND `Status` = 1
      AND favorite.CreatedDate > date(
        ADDDATE(now(), INTERVAL - 1 DAY)
      )
      AND favorite.CreatedDate < date(NOW())
    ) AS lastdayaddfavorite
    FROM
      ims_associate
    LEFT JOIN store ON store.id = ims_associate.storeid
    LEFT JOIN section ON section.id = ims_associate.sectionid
    LEFT JOIN ims_operator ON ims_associate.UserId = ims_operator.UserId
    LEFT JOIN ims_associate_apply ON ims_associate.UserId = ims_associate_apply.AssociateUserId
    join `user` on ims_associate.UserId = `user`.id
    where `user`.UserLevel = 4 or `user`.UserLevel = 16 or `user`.UserLevel = 8
    ORDER BY
      ims_associate.id
    "
  end
  def buyer_everyday_data_sql(time_begin, time_end)
    "SELECT
      ims_associate.UserId,
      store.Id as storeid,
      user.UserLevel as UserLevel,
      CASE
    WHEN ISNULL(
      ims_associate_apply.`StoreName`
    ) THEN
      store.`Name`
    ELSE
      ims_associate_apply.`StoreName`
    END AS storeName,
     section.Id,
     CASE
    WHEN ISNULL(
      ims_associate_apply.`SectionName`
    ) THEN
      section.`Name`
    ELSE
      ims_associate_apply.`SectionName`
    END AS SectionName,
     section.SectionCode,
     CASE
    WHEN ISNULL(ims_associate_apply.`Name`) THEN
      ims_operator.`Name`
    ELSE
      ims_associate_apply.`Name`
    END AS buyerName,
     (
      SELECT
        IFNULL(sum(totalamount), 0)
      FROM
        `order`
      JOIN ims_associateincomehistory ON `order`.OrderNo = ims_associateincomehistory.SourceNo
      WHERE
        ims_associateincomehistory.AssociateUserId = ims_associate.UserId
      AND `order`.`Status` > 0 and `order`.CreateDate < '#{time_end}' and `order`.CreateDate >= '#{time_begin}'
    ) AS orderAmount,
    (
      SELECT
        IFNULL( sum(RecAmount), 0)
      FROM
        `order`
      JOIN ims_associateincomehistory ON `order`.OrderNo = ims_associateincomehistory.SourceNo
      WHERE
        ims_associateincomehistory.AssociateUserId = ims_associate.UserId
      AND `order`.`Status` > 0  and `order`.CreateDate < '#{time_end}' and `order`.CreateDate >= '#{time_begin}'
    ) AS orderRecivedAmount,
    (
      SELECT
        count(*)
      FROM
        favorite
      WHERE
        FavoriteSourceType = 7
      AND FavoriteSourceId = ims_associate.id
      AND `Status` = 1
    ) AS allfavoriteNumber,
    (
      SELECT
        count(*)
      FROM
        favorite
      WHERE
        FavoriteSourceType = 7
      AND FavoriteSourceId = ims_associate.id
      AND `Status` = 1
      AND favorite.CreatedDate > date(
        ADDDATE(now(), INTERVAL - 1 DAY)
      )
      AND favorite.CreatedDate < date(NOW())
    ) AS lastdayaddfavorite
    FROM
      ims_associate
    LEFT JOIN store ON store.id = ims_associate.storeid
    LEFT JOIN section ON section.id = ims_associate.sectionid
    LEFT JOIN ims_operator ON ims_associate.UserId = ims_operator.UserId
    LEFT JOIN ims_associate_apply ON ims_associate.UserId = ims_associate_apply.AssociateUserId
    join `user` on ims_associate.UserId = `user`.id
    where `user`.UserLevel = 4 or `user`.UserLevel = 16 or `user`.UserLevel = 8
    ORDER BY
      ims_associate.id
    "
  end
end