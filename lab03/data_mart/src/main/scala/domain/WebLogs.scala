package domain

case class WebLogs(
                    uid: Option[String],
                    visits: Array[(Long, String)]
                  )
