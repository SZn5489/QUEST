package columnar;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.trevni.Input;
import org.apache.trevni.InputFile;
import org.apache.trevni.TrevniRuntimeException;

import io.InputBuffer;
import codec.metadata.FileColumnMetaData;
import codec.metadata.FileMetaData;

public class InsertColumnFileReader implements Closeable {
    protected Input headFile;
    protected Input dataFile;

    protected int rowCount;
    protected int columnCount;
    protected FileMetaData metaData;
    protected ColumnDescriptor[] columns;
    protected HashMap<String, Integer> columnsByName;

    public InsertColumnFileReader(File file) throws IOException {
        //输入的数据文件为neci文件
        this.dataFile = new InputFile(file);
        //头文件为neci文件对应的同名的.head文件
        this.headFile = new InputFile(
                new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head"));
        readHeader();
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public FileMetaData getMetaData() {
        return metaData;
    }

    /**
     * Return all columns' metadata.
     */
    public FileColumnMetaData[] getFileColumnMetaData() {
        FileColumnMetaData[] result = new FileColumnMetaData[columnCount];
        for (int i = 0; i < columnCount; i++)
            result[i] = columns[i].metaData;
        return result;
    }

    public List<FileColumnMetaData> getRoots() {
        List<FileColumnMetaData> result = new ArrayList<FileColumnMetaData>();
        for (int i = 0; i < columnCount; i++)
            if (columns[i].metaData.getParent() == null)
                result.add(columns[i].metaData);
        return result;
    }

    public FileColumnMetaData getFileColumnMetaData(int number) {
        return columns[number].metaData;
    }

    /**
     * Return a column's metadata.
     */
    public FileColumnMetaData getFileColumnMetaData(String name) {
        return getColumn(name).metaData;
    }

    public int getColumnNumber(String name) {
        if ((columnsByName.get(name)) == null)
            throw new TrevniRuntimeException("No column named: " + name);
        return columnsByName.get(name);
    }

    private <T extends Comparable> ColumnDescriptor<T> getColumn(String name) {
        return (ColumnDescriptor<T>) columns[getColumnNumber(name)];
    }

    /**
     * 读取.head头文件
     * @throws IOException ...
     */
    private void readHeader() throws IOException {
        //头文件输入缓冲区
        InputBuffer in = new InputBuffer(headFile, 0);
        readMagic(in);//读取4个字节判断是否为neci文件
        this.rowCount = in.readFixed32(); //行数
        this.columnCount = in.readFixed32(); //列数
        this.metaData = FileMetaData.read(in); //获取元数据
        this.columnsByName = new HashMap<String, Integer>(columnCount);

        columns = new ColumnDescriptor[columnCount];
        // 从in中读入数据
        readFileColumnMetaData(in);
        //初始化每列的数据开始的全局index
        readColumnStarts(in);
    }

    public HashMap<String, Integer> getColumnsByName() {
        return columnsByName;
    }

    /**
     * 判断输入文件流是否为neci文件
     * 读取文件的4个字节并和{ 'N', 'E', 'C', 'I' }比较，相同为neci文件
     * @param in 输入的文件的缓冲区
     * @throws IOException 文件不是neci文件时抛出
     */
    protected void readMagic(InputBuffer in) throws IOException {
        //InsertColumnFileWriter.MAGIC = new byte[] { 'N', 'E', 'C', 'I' };
        byte[] magic = new byte[InsertColumnFileWriter.MAGIC.length];
        try {
            in.readFully(magic);
        } catch (IOException e) {
            throw new IOException("Not a neci file.");
        }
        if (!(Arrays.equals(InsertColumnFileWriter.MAGIC, magic)))
            throw new IOException("Not a neci file.");
    }

    /**
     * 从in中读入数据，初始化block descriptor 和 column descriptor
     * @param in 输入的数据文件
     * @throws IOException ...
     */
    protected void readFileColumnMetaData(InputBuffer in) throws IOException {
        for (int i = 0; i < columnCount; i++) {
            FileColumnMetaData meta = FileColumnMetaData.read(in, this);
            meta.setDefaults(this.metaData);
            int blockCount = in.readFixed32();
            CompressedBlockDescriptor[] blocks = new CompressedBlockDescriptor[blockCount];
            for (int j = 0; j < blockCount; j++) {
                blocks[j] = CompressedBlockDescriptor.read(in);
                //          if (meta.hasIndexValues())
                //          firstValues[i] = in.<T>readValue(meta.getType());
            }
            ColumnDescriptor column = new ColumnDescriptor(dataFile, meta);
            column.setBlockDescriptor(blocks);
            columns[i] = column;
            meta.setNumber(i);
            columnsByName.put(meta.getName(), i);
        }
    }

    /**
     * 读取每列的开始全局index
     * @param in 输入数据文件
     * @throws IOException ...
     */
    protected void readColumnStarts(InputBuffer in) throws IOException {
        for (int i = 0; i < columnCount; i++)
            columns[i].start = in.readFixed64();
    }

    public <T extends Comparable> ColumnValues<T> getValues(String columnName) throws IOException {
        return new ColumnValues<T>(getColumn(columnName));
    }

    public <T extends Comparable> ColumnValues<T> getValues(int column) throws IOException {
        return new ColumnValues<T>(columns[column]);
    }

    @Override
    public void close() throws IOException {
        headFile.close();
        dataFile.close();
    }

}
