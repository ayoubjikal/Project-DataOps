

module "storage" {
  source        = "./modules/storage"
  buckets_names = ["anass-bucket-dataops-test", "ayoub-bucket-dataops-test"]

}

module "iam_ayoub" {
  source       = "./modules/iam"
  user_name    = "dataops-ayoub"
  bucket_names = ["ayoub-bucket-dataops-test"]
}
