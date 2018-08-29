package configuration

case class Configuration(inputFiles: InputFiles, geoData: GeoData, processing: Processing, output: Output)

case class InputFiles(
  basePath: String,
  inputCities: String
)

case class GeoData(
  basePath: String,
  geoCities: String
)

case class Processing(
  logfilePrefix: String,
  logfileSuffix: String,
  loggingProperties: String,
  showDataframeSample: Boolean
)

case class Output(
  path: String
)