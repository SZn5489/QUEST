package cores.avro;

public interface FilterOperator<T> {
    /**
     * 获得filter的过滤字段名称
     * @return 返回该filter作用的字段名称
     */
    public String getName();

    /**
     * 判断输入的t是否满足该filter的过滤条件
     * @param t 输入
     * @return
     */
    public boolean isMatch(T t);
}
