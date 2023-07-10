package cores.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import columnar.BlockDescriptor;
import columnar.InsertColumnFileWriter;
import io.OutputBuffer;
import codec.metadata.FileColumnMetaData;
import codec.metadata.FileMetaData;

public class AvroColumnWriter {
    protected FileColumnMetaData[] meta;
    protected FileMetaData filemeta;
    protected OutputStream data;
    protected OutputStream head;
    protected int rowcount;
    protected int columncount;
    protected long[] columnStart;
    protected Blocks[] blocks;
    OutputBuffer buf;
    int index;

    public static final String SCHEMA_KEY = "avro.schema";

    class Blocks {
        private final List<BlockDescriptor> blocks;

        Blocks() {
            blocks = new ArrayList<BlockDescriptor>();
        }

        List<BlockDescriptor> get() {
            return blocks;
        }

        void add(BlockDescriptor b) {
            blocks.add(b);
        }

        void clear() {
            blocks.clear();
        }

        BlockDescriptor get(int i) {
            return blocks.get(i);
        }
    }

    // public static void MemPrint(){
    //     System.out.println("$$$$$$$$$\t"+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    // }

    //  public InsertColumnFileWriter(File fromfile, ListArr[] sort) throws IOException {
    //    this.reader = new InsertColumnFileReader(fromfile);
    //    this.insert = sort;
    //    this.filemeta = reader.getMetaData();
    //    this.meta = reader.getFileColumnMetaData();
    //    this.addRow = sort[0].size();
    //  }

    public AvroColumnWriter(Schema schema, String path) throws IOException {
        AvroColumnator columnator = new AvroColumnator(schema);
        filemeta = new FileMetaData();
        filemeta.set(SCHEMA_KEY, schema.toString());
        meta = columnator.getColumns();
        this.columncount = meta.length;
        this.columnStart = new long[columncount];
        this.blocks = new Blocks[columncount];
        for (int i = 0; i < columncount; i++) {
            blocks[i] = new Blocks();
        }
        data = new FileOutputStream(new File(path));
        head = new FileOutputStream(new File(path.substring(0, path.lastIndexOf(".")) + ".head"));
        buf = new OutputBuffer();
        index = 0;
        rowcount = 0;
    }

    public void writeColumn(int columnNo, Object value) throws IOException {
        if (buf.isFull()) {
            BlockDescriptor b = new BlockDescriptor(index, buf.size(), buf.size());
            blocks[columnNo].add(b);
            index = 0;
            buf.writeTo(data);
            buf.reset();
        }
        //        ValueType tp = meta[columnNo].getType();
        //        switch (tp) {
        //            case FIXED32:
        //            case INT:
        //                buf.writeValue(Integer.parseInt(value.toString()), tp);
        //                break;
        //            case FIXED64:
        //            case LONG:
        //                buf.writeValue(Long.parseLong(value.toString()), tp);
        //                break;
        //            case FLOAT:
        //                buf.writeValue(Float.parseFloat(value.toString()), tp);
        //                break;
        //            case DOUBLE:
        //                buf.writeValue(Double.parseDouble(value.toString()), tp);
        //                break;
        //            default:
        //                buf.writeValue(value, tp);
        //                break;
        //        }
        buf.writeValue(value, meta[columnNo].getType());
        index++;
        if (columnNo == 0) {
            rowcount++;
        }
    }

    public void writeArrayColumn(int columnNo, int value) throws IOException {
        if (buf.isFull()) {
            BlockDescriptor b = new BlockDescriptor(index, buf.size(), buf.size());
            blocks[columnNo].add(b);
            index = 0;
            buf.writeTo(data);
            buf.reset();
        }
        buf.writeLength(value);
        index++;
    }

    public void flush(int columnNo) throws IOException {
        if (index > 0) {
            BlockDescriptor b = new BlockDescriptor(index, buf.size(), buf.size());
            blocks[columnNo].add(b);
            index = 0;
            buf.writeTo(data);
            buf.reset();
        }
    }

    public void close() throws IOException {
        writeHeader();
    }

    public void writeHeader() throws IOException {
        buf.write(InsertColumnFileWriter.MAGIC);
        buf.writeFixed32(rowcount);
        buf.writeFixed32(columncount);
        filemeta.write(buf);
        int i = 0;
        long delay = 0;
        for (FileColumnMetaData c : meta) {
            columnStart[i] = delay;
            c.write(buf);
            int size = blocks[i].blocks.size();
            buf.writeFixed32(size);
            for (int k = 0; k < size; k++) {
                blocks[i].get(k).writeTo(buf);
                delay += blocks[i].get(k).getSize();
            }
            blocks[i].clear();
            i++;
        }

        for (i = 0; i < columncount; i++) {
            buf.writeFixed64(columnStart[i]);
        }
        buf.writeTo(head);
        buf.close();
    }
}
