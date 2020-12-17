package domain

case class OnlineLogs(
                       uid: Option[String],
                       event_type: Option[String],
                       category: Option[String],
                       item_id: Option[String],
                       item_price: Option[Long],
                       timestamp: Option[Long]
                     )
