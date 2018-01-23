namespace hpmr {

template <class K, class V, class H>
class DistMap {
 public:
  void set(const K& key, const V& value);

  void map() {}

  void reduce() {}

 private:
};

template <class K, class V, class H>
void DistMap<K, V, H>::set(const K& key, const V& value) {}

}  // namespace hpmr
