package com.atguigu.gmall0213.realtime.bean

case class OrderWide(
                            var order_detail_id:Long =0L,
                            var order_id: Long=0L,
                            var order_status:String=null,
                            var create_time:String=null,
                            var user_id: Long=0L,
                            var sku_id: Long=0L,
                            var sku_price: Double=0D,
                            var sku_num: Long=0L,
                            var sku_name: String=null,
                            var benefit_reduce_amount:Double =0D ,
                            var original_total_amount:Double =0D ,
                            var feight_fee:Double=0D,
                            var final_total_amount: Double =0D ,
                            var final_detail_amount:Double=0D,

                            var if_first_order:String=null,

                            var province_name:String=null,
                            var province_area_code:String=null,

                            var user_age_group:String=null,
                            var user_gender:String=null,

                            var dt:String=null,

                            var spu_id: Long=0L,
                            var tm_id: Long=0L,
                            var category3_id: Long=0L,
                            var spu_name: String=null,
                            var tm_name: String=null,
                            var category3_name: String=null


                          ) {
  def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)

  }

  def mergeOrderInfo(orderInfo: OrderInfo): Unit = {
    if (orderInfo != null) {
      this.order_id = orderInfo.id
      this.order_status = orderInfo.order_status
      this.create_time = orderInfo.create_time
      this.dt = orderInfo.create_date

      this.benefit_reduce_amount = orderInfo.benefit_reduce_amount
      this.original_total_amount = orderInfo.original_total_amount
      this.feight_fee = orderInfo.feight_fee
      this.final_total_amount = orderInfo.final_total_amount


      this.province_name = orderInfo.province_name
      this.province_area_code = orderInfo.province_area_code

      this.user_age_group = orderInfo.user_age_group
      this.user_gender = orderInfo.user_gender

      this.if_first_order = orderInfo.if_first_order

      this.user_id = orderInfo.user_id
    }
  }


  def mergeOrderDetail(orderDetail: OrderDetail): Unit = {
    if (orderDetail != null) {
      this.order_detail_id = orderDetail.id
      this.sku_id = orderDetail.sku_id
      this.sku_name = orderDetail.sku_name
      this.sku_price = orderDetail.order_price
      this.sku_num = orderDetail.sku_num

      this.spu_id = orderDetail.spu_id
      this.tm_id = orderDetail.tm_id
      this.category3_id = orderDetail.category3_id
      this.spu_name = orderDetail.spu_name
      this.tm_name = orderDetail.tm_name
      this.category3_name = orderDetail.category3_name

    }
  }
}
