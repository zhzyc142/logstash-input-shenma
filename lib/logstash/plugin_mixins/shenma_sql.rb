module LogStash::PluginMixins::ShenmaSql
  public 
  def buyer_everyday_data_sql(time_begin, time_end)
    "SELECT
      ims_associate.UserId,
      store.Id as storeid,
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