package sparkly.component.preprocess

case class CategoryIndex(index: Map[String, Int] = Map()) {
  def apply(category: String) = index(category)

  def add(category: String): CategoryIndex = index.contains(category) match {
    case true => this
    case false => this.copy(index = index + (category -> index.size))
  }

  def add(categories: Iterable[String]): CategoryIndex = categories.foldLeft(this)((current, category) => current.add(category))
}